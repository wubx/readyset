import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * databend-jdbc compatibility probe, used the way the production client uses it: DriverManager
 * connections (no pool), PreparedStatement with bound parameters, reads and writes on the same
 * URL. Every read step runs on the gateway and on Databend directly and the rows are compared;
 * gateway /health counters before/after each step tell local vs forwarded.
 *
 * Args: gatewayJdbcUrl databendJdbcUrl healthUrl user password table keySpace loadSecs loadThreads
 *       [loadTarget: gateway (default) | databend] [writeIntervalMs: 0 = reads only]
 * Output: one JSON object per line (step results), last line "SUMMARY {...}".
 */
public class JdbcCompatProbe {
    static final String COLS = "id, ext_id, i32, u8, flag, amount, note";
    static String user, password, health, table;
    static final HttpClient HTTP = HttpClient.newHttpClient();
    static int passed = 0, failed = 0;

    static String q(Object o) {
        if (o == null) return "null";
        String s = o.toString();
        StringBuilder b = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            if (c == '"' || c == '\\') b.append('\\').append(c);
            else if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
            else b.append(c);
        }
        return b.append('"').toString();
    }

    /** counters.readyset / counters.databend from the gateway /health. */
    static long[] counters() throws Exception {
        String auth = Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
        HttpRequest r = HttpRequest.newBuilder(URI.create(health)).header("Authorization", "Basic " + auth).build();
        String body = HTTP.send(r, HttpResponse.BodyHandlers.ofString()).body();
        int at = body.indexOf("\"counters\"");
        String c = body.substring(at);
        return new long[]{num(c, "readyset"), num(c, "databend")};
    }

    static long num(String s, String key) {
        Matcher m = Pattern.compile("\"" + key + "\":(\\d+)").matcher(s);
        return m.find() ? Long.parseLong(m.group(1)) : -1;
    }

    /** Rows rendered with type-specific getters, plus column labels and JDBC types. */
    static List<String> rows(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        List<String> out = new ArrayList<>();
        StringBuilder meta = new StringBuilder("meta:");
        for (int i = 1; i <= md.getColumnCount(); i++) {
            meta.append(md.getColumnLabel(i)).append('/').append(md.getColumnType(i)).append('/')
                .append(md.getColumnTypeName(i)).append('/').append(md.isNullable(i)).append(';');
        }
        out.add(meta.toString());
        while (rs.next()) {
            StringBuilder b = new StringBuilder();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                Object v = rs.getObject(i);
                b.append(rs.wasNull() ? "NULL" : v.getClass().getSimpleName() + ":" + v).append('|');
            }
            out.add(b.toString());
        }
        return out;
    }

    interface Binder { void bind(PreparedStatement p) throws SQLException; }

    static List<String> query(Connection c, String sql, Binder b) throws SQLException {
        try (PreparedStatement p = c.prepareStatement(sql)) {
            b.bind(p);
            try (ResultSet rs = p.executeQuery()) { return rows(rs); }
        }
    }

    static int update(Connection c, String sql, Binder b) throws SQLException {
        try (PreparedStatement p = c.prepareStatement(sql)) {
            b.bind(p);
            return p.executeUpdate();
        }
    }

    static void report(String step, boolean ok, long[] before, String detail) throws Exception {
        long[] after = counters();
        if (ok) passed++; else failed++;
        System.out.println("{\"step\":" + q(step) + ",\"ok\":" + ok + ",\"local\":" + (after[0] - before[0])
            + ",\"forwarded\":" + (after[1] - before[1]) + ",\"detail\":" + q(detail) + "}");
        System.out.flush();
    }

    /** Run one step; an exception is a failed step, never an aborted probe. */
    interface Step { String run() throws Exception; }

    static void step(String name, Step s) throws Exception {
        long[] before = counters();
        try {
            String detail = s.run();
            report(name, detail == null || !detail.startsWith("FAIL"), before, detail == null ? "" : detail);
        } catch (Throwable e) {
            String m = e.toString();
            Throwable c = e.getCause();
            if (c != null) m += " / cause: " + c;
            report(name, false, before, "FAIL exception: " + (m.length() > 1500 ? m.substring(0, 1500) : m));
        }
    }

    static Connection connect(String url) throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", password);
        return DriverManager.getConnection(url, p);
    }

    static String cmp(List<String> got, List<String> want) {
        return got.equals(want) ? null : "FAIL mismatch gateway=" + got + " databend=" + want;
    }

    public static void main(String[] a) throws Exception {
        String gwUrl = a[0], dbUrl = a[1];
        health = a[2]; user = a[3]; password = a[4]; table = a[5];
        long keySpace = Long.parseLong(a[6]);
        int loadSecs = Integer.parseInt(a[7]), loadThreads = Integer.parseInt(a[8]);
        String loadUrl = a.length > 9 && a[9].equals("databend") ? dbUrl : gwUrl;
        long writeEvery = a.length > 10 ? Long.parseLong(a[10]) : 0;
        String byId = "SELECT " + COLS + " FROM " + table + " WHERE id = ?";
        String byExt = "SELECT " + COLS + " FROM " + table + " WHERE ext_id = ?";
        Connection[] gw = new Connection[1];
        Connection ref = connect(dbUrl);

        step("connect", () -> {
            gw[0] = connect(gwUrl);
            DatabaseMetaData md = gw[0].getMetaData();
            return "driver " + md.getDriverVersion() + ", server " + md.getDatabaseProductVersion();
        });
        if (gw[0] == null) { summary(); return; }
        Connection g = gw[0];

        // Fixed keys: ordinary rows, rows with NULLs and special text, a missing key.
        long[] keys = {1, 2, 3, 7, 42, 999, keySpace - 1, keySpace + 12345};
        step("pk_lookup_prepared_reused", () -> {
            try (PreparedStatement pg = g.prepareStatement(byId); PreparedStatement pr = ref.prepareStatement(byId)) {
                for (long k : keys) {
                    pg.setLong(1, k); pr.setLong(1, k);
                    List<String> x, y;
                    try (ResultSet r = pg.executeQuery()) { x = rows(r); }
                    try (ResultSet r = pr.executeQuery()) { y = rows(r); }
                    String bad = cmp(x, y);
                    if (bad != null) return bad + " key=" + k;
                }
            }
            return keys.length + " keys equal";
        });
        step("ext_lookup_setString", () -> {
            for (long k : new long[]{1, 3, 5, 77}) {
                String ext = (String) query(ref, "SELECT ext_id FROM " + table + " WHERE id = ?", p -> p.setLong(1, k))
                    .get(1).split("\\|")[0].split(":", 2)[1];
                String bad = cmp(query(g, byExt, p -> p.setString(1, ext)), query(ref, byExt, p -> p.setString(1, ext)));
                if (bad != null) return bad;
            }
            String bad = cmp(query(g, byExt, p -> p.setString(1, "no'such\\key")), query(ref, byExt, p -> p.setString(1, "no'such\\key")));
            return bad != null ? bad : "4 keys + quoted missing key equal";
        });
        step("typed_getters", () -> {
            StringBuilder all = new StringBuilder();
            for (long k : new long[]{2, 3}) {
                String[] seen = new String[2];
                Connection[] cs = {g, ref};
                for (int i = 0; i < 2; i++) {
                    try (PreparedStatement p = cs[i].prepareStatement(byId)) {
                        p.setLong(1, k);
                        try (ResultSet r = p.executeQuery()) {
                            if (!r.next()) return "FAIL no row for key " + k;
                            StringBuilder b = new StringBuilder();
                            b.append(r.getLong(1)).append(',').append(r.getString(2)).append(',').append(r.getInt(3))
                                .append(r.wasNull() ? "(null)" : "").append(',').append(r.getShort(4)).append(',')
                                .append(r.getBoolean(5)).append(',').append(r.getBigDecimal(6)).append(',')
                                .append(r.getString(7)).append(r.wasNull() ? "(null)" : "");
                            seen[i] = b.toString();
                        }
                    }
                }
                if (!seen[0].equals(seen[1])) return "FAIL key " + k + " gateway=" + seen[0] + " databend=" + seen[1];
                all.append(seen[0]).append("; ");
            }
            return all.toString();
        });
        step("statement_literal", () -> {
            String sql = "SELECT " + COLS + " FROM " + table + " WHERE id = 42";
            try (Statement s1 = g.createStatement(); Statement s2 = ref.createStatement();
                 ResultSet r1 = s1.executeQuery(sql); ResultSet r2 = s2.executeQuery(sql)) {
                String bad = cmp(rows(r1), rows(r2));
                return bad != null ? bad : "equal";
            }
        });
        step("uncached_aggregate", () -> {
            String sql = "SELECT count(*), sum(amount) FROM " + table + " WHERE id < ?";
            String bad = cmp(query(g, sql, p -> p.setLong(1, 1000)), query(ref, sql, p -> p.setLong(1, 1000)));
            return bad != null ? bad : "equal";
        });
        step("database_metadata", () -> {
            StringBuilder b = new StringBuilder();
            for (Connection c : new Connection[]{g, ref}) {
                int cols = 0;
                try (ResultSet r = c.getMetaData().getColumns(null, c.getSchema(), table, null)) { while (r.next()) cols++; }
                b.append(cols).append(' ');
            }
            String[] n = b.toString().trim().split(" ");
            return n[0].equals(n[1]) && !n[0].equals("0") ? "columns " + n[0] : "FAIL columns gateway/databend " + b;
        });

        step("connect_per_query_x50", () -> {
            long t0 = System.nanoTime();
            for (int i = 0; i < 50; i++) {
                long k = 1 + i * 997L % keySpace;
                try (Connection c = connect(gwUrl)) {
                    List<String> x = query(c, byId, p -> p.setLong(1, k));
                    if (x.size() != 2) return "FAIL key " + k + " rows " + x;
                }
            }
            return String.format("avg %.1f ms per connect+query", (System.nanoTime() - t0) / 1e6 / 50);
        });
        step("arrow_format_requested_falls_back_to_json", () -> {
            // query_result_format=arrow: the gateway login does not advertise Arrow, so the
            // driver stays on JSON and point lookups can still be served locally.
            String sep = gwUrl.contains("?") ? "&" : "?";
            try (Connection c = connect(gwUrl + sep + "query_result_format=arrow")) {
                long[] c0 = counters();
                List<String> x = query(c, byId, p -> p.setLong(1, 7));
                long[] c1 = counters();
                String bad = cmp(x, query(ref, byId, p -> p.setLong(1, 7)));
                if (bad != null) return bad;
                return c1[0] - c0[0] == 1 ? "equal, served locally" : "FAIL not local";
            }
        });
        // Writes through the gateway, read back immediately on the same connection.
        long wid = keySpace + 1_000_000 + new Random().nextInt(1_000_000);
        String ins = "INSERT INTO " + table + " (" + COLS + ") VALUES (?, ?, ?, ?, ?, ?, ?)";
        Binder row = p -> {
            p.setLong(1, wid); p.setString(2, "jdbc-" + wid); p.setNull(3, java.sql.Types.INTEGER);
            p.setInt(4, 7); p.setBoolean(5, true); p.setBigDecimal(6, new BigDecimal("12.30")); p.setString(7, "它's \"q\" \\ 😀");
        };
        step("insert_then_read_own_write", () -> {
            int n = update(g, ins, row);
            List<String> x = query(g, byId, p -> p.setLong(1, wid));
            if (n != 1) return "FAIL update count " + n;
            String bad = cmp(x, query(ref, byId, p -> p.setLong(1, wid)));
            return bad != null ? bad : (x.size() == 2 ? "visible at once" : "FAIL own write not visible: " + x);
        });
        step("update_then_read_own_write", () -> {
            int n = update(g, "UPDATE " + table + " SET amount = ? WHERE id = ?", p -> { p.setBigDecimal(1, new BigDecimal("99.99")); p.setLong(2, wid); });
            List<String> x = query(g, byId, p -> p.setLong(1, wid));
            if (n != 1) return "FAIL update count " + n;
            return x.size() == 2 && x.get(1).contains("99.99") ? "visible at once" : "FAIL " + x;
        });
        step("local_after_sync", () -> {
            long t0 = System.nanoTime();
            while (System.nanoTime() - t0 < 30_000_000_000L) {
                long[] c0 = counters();
                List<String> x = query(g, byId, p -> p.setLong(1, wid));
                long[] c1 = counters();
                if (c1[0] - c0[0] == 1) {
                    String bad = cmp(x, query(ref, byId, p -> p.setLong(1, wid)));
                    return bad != null ? bad : String.format("local and equal after %.2fs", (System.nanoTime() - t0) / 1e9);
                }
                Thread.sleep(100);
            }
            return "FAIL never local within 30s";
        });
        step("delete_then_read_own_write", () -> {
            int n = update(g, "DELETE FROM " + table + " WHERE id = ?", p -> p.setLong(1, wid));
            List<String> x = query(g, byId, p -> p.setLong(1, wid));
            return x.size() == 1 ? "gone at once (deleted " + n + ")" : "FAIL still visible: " + x;
        });
        // databend-jdbc: setAutoCommit(false) does not open a transaction (it only sets a
        // driver-side flag); a transaction starts with an explicit BEGIN. Both are compared
        // with the same calls on Databend directly.
        step("auto_commit_false_matches_databend", () -> {
            long id = wid + 1;
            g.setAutoCommit(false);
            try {
                update(g, ins, p -> { row.bind(p); p.setLong(1, id); });
                List<String> other = query(ref, byId, p -> p.setLong(1, id));
                g.commit();
                List<String> mine = query(g, byId, p -> p.setLong(1, id));
                return other.size() == 2 && mine.size() == 2
                    ? "statement autocommitted, same as Databend" : "FAIL other=" + other + " mine=" + mine;
            } finally { g.setAutoCommit(true); }
        });
        step("explicit_begin_commit", () -> {
            long id = wid + 2;
            try (Statement s = g.createStatement()) { s.execute("BEGIN"); }
            update(g, ins, p -> { row.bind(p); p.setLong(1, id); });
            List<String> inside = query(g, byId, p -> p.setLong(1, id));
            List<String> other = query(ref, byId, p -> p.setLong(1, id));
            g.commit();
            List<String> after = query(g, byId, p -> p.setLong(1, id));
            if (inside.size() != 2) return "FAIL own uncommitted row invisible inside txn";
            if (other.size() != 1) return "FAIL uncommitted row visible to another session";
            return after.size() == 2 ? "own row visible in txn, isolated, visible after commit" : "FAIL lost after commit";
        });
        step("explicit_begin_rollback", () -> {
            long id = wid + 3;
            try (Statement s = g.createStatement()) { s.execute("BEGIN"); }
            update(g, ins, p -> { row.bind(p); p.setLong(1, id); });
            g.rollback();
            List<String> x = query(g, byId, p -> p.setLong(1, id));
            List<String> y = query(ref, byId, p -> p.setLong(1, id));
            return x.size() == 1 && y.size() == 1 ? "rolled back" : "FAIL gateway=" + x + " databend=" + y;
        });
        step("read_in_txn_is_forwarded_and_equal", () -> {
            try (Statement s = g.createStatement()) { s.execute("BEGIN"); }
            try {
                long[] c0 = counters();
                List<String> x = query(g, byId, p -> p.setLong(1, 42));
                long[] c1 = counters();
                String bad = cmp(x, query(ref, byId, p -> p.setLong(1, 42)));
                if (bad != null) return bad;
                return c1[0] == c0[0] ? "forwarded and equal" : "FAIL served locally inside a transaction";
            } finally { g.commit(); }
        });
        step("temp_table_shadows_cached_table", () -> {
            // A session temp table named like the cached table hides it for that session only.
            String[] seen = new String[2];
            String[] urls = {gwUrl, dbUrl};
            for (int i = 0; i < 2; i++) {
                try (Connection c = connect(urls[i]); Statement s = c.createStatement()) {
                    s.execute("CREATE TEMP TABLE " + table + " (id BIGINT, ext_id VARCHAR, i32 INT, u8 TINYINT UNSIGNED, "
                        + "flag BOOLEAN, amount DECIMAL(18,2), note STRING)");
                    s.execute("INSERT INTO " + table + " VALUES (42, 'temp', 1, 1, false, 0.01, 'temp row')");
                    seen[i] = String.valueOf(query(c, byId, p -> p.setLong(1, 42)));
                }
            }
            if (!seen[0].equals(seen[1])) return "FAIL gateway=" + seen[0] + " databend=" + seen[1];
            if (!seen[0].contains("temp row")) return "FAIL temp table not visible: " + seen[0];
            String after = cmp(query(g, byId, p -> p.setLong(1, 42)), query(ref, byId, p -> p.setLong(1, 42)));
            return after != null ? after : "temp row seen in its session; other sessions still see the cached row";
        });
        step("batch_insert", () -> {
            long id = wid + 10;
            try (PreparedStatement p = g.prepareStatement(ins)) {
                for (int i = 0; i < 3; i++) { row.bind(p); p.setLong(1, id + i); p.setString(2, "batch-" + (id + i)); p.addBatch(); }
                int[] r = p.executeBatch();
                List<String> y = query(ref, "SELECT count(*) FROM " + table + " WHERE id BETWEEN ? AND ?", b -> { b.setLong(1, id); b.setLong(2, id + 2); });
                return y.get(1).contains(":3|") ? "3 rows, counts " + java.util.Arrays.toString(r) : "FAIL databend sees " + y;
            }
        });
        step("close", () -> { g.close(); return g.isClosed() ? "closed" : "FAIL not closed"; });

        // Load: each thread owns one connection and one reused PreparedStatement.
        if (loadSecs > 0) {
            long[] before = counters();
            AtomicLong ok = new AtomicLong(), bad = new AtomicLong(), lat = new AtomicLong();
            List<String> errors = java.util.Collections.synchronizedList(new ArrayList<>());
            List<Long> samples = java.util.Collections.synchronizedList(new ArrayList<>());
            long until = System.nanoTime() + loadSecs * 1_000_000_000L;
            List<Thread> ts = new ArrayList<>();
            for (int t = 0; t < loadThreads; t++) {
                final int seed = t;
                Thread th = new Thread(() -> {
                    Random rnd = new Random(seed);
                    try (Connection c = connect(loadUrl); PreparedStatement p = c.prepareStatement(byId)) {
                        while (System.nanoTime() < until) {
                            long k = rnd.nextInt((int) Math.min(keySpace, Integer.MAX_VALUE));
                            long s = System.nanoTime();
                            try {
                                p.setLong(1, k);
                                int n = 0;
                                try (ResultSet r = p.executeQuery()) { while (r.next()) n++; }
                                long d = System.nanoTime() - s;
                                lat.addAndGet(d);
                                if (rnd.nextInt(50) == 0) samples.add(d);
                                if (n == 1) ok.incrementAndGet(); else { bad.incrementAndGet(); if (errors.size() < 5) errors.add("rows " + n + " key " + k); }
                            } catch (SQLException e) {
                                bad.incrementAndGet();
                                if (errors.size() < 5) errors.add(e.toString());
                            }
                        }
                    } catch (SQLException e) { if (errors.size() < 5) errors.add("connect: " + e); }
                });
                th.start(); ts.add(th);
            }
            // Optional writer on the same URL, as an application mixing reads and writes.
            AtomicLong writes = new AtomicLong(), writeNanos = new AtomicLong();
            Thread writer = null;
            if (writeEvery > 0) {
                writer = new Thread(() -> {
                    long next = wid + 100_000;
                    try (Connection c = connect(loadUrl); PreparedStatement p = c.prepareStatement(ins)) {
                        while (System.nanoTime() < until) {
                            long s = System.nanoTime();
                            row.bind(p); p.setLong(1, next); p.setString(2, "mix-" + next++);
                            p.executeUpdate();
                            writes.incrementAndGet();
                            writeNanos.addAndGet(System.nanoTime() - s);
                            long sleep = writeEvery - (System.nanoTime() - s) / 1_000_000;
                            if (sleep > 0) Thread.sleep(sleep);
                        }
                    } catch (Exception e) { if (errors.size() < 5) errors.add("writer: " + e); }
                });
                writer.start();
            }
            for (Thread th : ts) th.join();
            if (writer != null) writer.join();
            List<Long> s = new ArrayList<>(samples);
            java.util.Collections.sort(s);
            double p50 = s.isEmpty() ? 0 : s.get(s.size() / 2) / 1e6, p99 = s.isEmpty() ? 0 : s.get((int) (s.size() * 0.99)) / 1e6;
            String detail = String.format("target " + (loadUrl == dbUrl ? "databend" : "gateway")
                + (writeEvery > 0 ? String.format(", writes %d (every %d ms, avg %.1f ms)", writes.get(), writeEvery, writeNanos.get() / 1e6 / Math.max(1, writes.get())) : "")
                + ", threads %d, qps %.0f, ok %d, bad %d, avg %.2f ms, p50 %.2f ms, p99 %.2f ms, errors %s",
                loadThreads, ok.get() / (double) loadSecs, ok.get(), bad.get(), lat.get() / 1e6 / Math.max(1, ok.get() + bad.get()), p50, p99, errors);
            report("load_prepared_point_lookup", bad.get() == 0 && ok.get() > 0, before, detail);
        }
        ref.close();
        summary();
    }

    static void summary() {
        System.out.println("SUMMARY {\"passed\":" + passed + ",\"failed\":" + failed + "}");
    }
}
