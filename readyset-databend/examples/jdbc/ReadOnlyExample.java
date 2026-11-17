import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal read-only databend-jdbc example against readyset-databend.
 *
 * Connects to readyset-databend (not to Databend), runs one PreparedStatement point lookup,
 * and tells from the service's /health counters whether it was answered from the local cache.
 * The cache must exist first, for example:
 *   CREATE CACHE orders_by_id FROM SELECT id, status, amount FROM shop.orders WHERE id = ?
 *
 * Environment (defaults in brackets):
 *   READYSET_JDBC_URL    [jdbc:databend://127.0.0.1:8001/shop]  service address; the path is the database
 *   READYSET_HEALTH_URL  [http://127.0.0.1:8001/health]
 *   READYSET_USER        [readyset]
 *   DATABEND_PASSWORD    []
 *   EXAMPLE_SQL          [SELECT id, status, amount FROM orders WHERE id = ?]  one long parameter
 *   EXAMPLE_ID           [42]
 */
public class ReadOnlyExample {
    static final String URL = env("READYSET_JDBC_URL", "jdbc:databend://127.0.0.1:8001/shop");
    static final String HEALTH = env("READYSET_HEALTH_URL", "http://127.0.0.1:8001/health");
    static final String USER = env("READYSET_USER", "readyset");
    static final String PASSWORD = env("DATABEND_PASSWORD", "");
    // Must match the CREATE CACHE shape: same columns, same table, WHERE id = ?
    static final String SQL = env("EXAMPLE_SQL", "SELECT id, status, amount FROM orders WHERE id = ?");
    static final long ID = Long.parseLong(env("EXAMPLE_ID", "42"));

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);

        try (Connection conn = DriverManager.getConnection(URL, props);
             PreparedStatement ps = conn.prepareStatement(SQL)) {
            long[] before = counters();
            ps.setLong(1, ID);
            int rows = 0;
            try (ResultSet rs = ps.executeQuery()) {
                int columns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    rows++;
                    StringBuilder line = new StringBuilder("row:");
                    for (int i = 1; i <= columns; i++) {
                        line.append(' ').append(rs.getMetaData().getColumnLabel(i)).append('=').append(rs.getString(i));
                    }
                    System.out.println(line);
                }
            }
            long[] after = counters();
            System.out.println("rows: " + rows);
            // Exact only while nothing else uses the service: the counters are instance-wide.
            if (after[0] - before[0] == 1 && after[1] == before[1]) {
                System.out.println("route: local cache (readyset)");
            } else {
                System.out.println("route: forwarded to Databend; see counters.forward_reasons in " + HEALTH);
            }
        } catch (SQLException e) {
            System.err.println("query failed: " + e.getMessage());
            System.exit(1);
        }
    }

    /** counters.readyset (answered locally) and counters.databend (forwarded) from /health. */
    static long[] counters() throws Exception {
        String auth = Base64.getEncoder().encodeToString((USER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
        HttpRequest req = HttpRequest.newBuilder(URI.create(HEALTH)).header("Authorization", "Basic " + auth).build();
        HttpResponse<String> resp = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("/health returned " + resp.statusCode());
        }
        String body = resp.body();
        String counters = body.substring(body.indexOf("\"counters\""));
        return new long[]{number(counters, "readyset"), number(counters, "databend")};
    }

    static long number(String json, String key) {
        Matcher m = Pattern.compile("\"" + key + "\":(\\d+)").matcher(json);
        if (!m.find()) {
            throw new IllegalStateException("counters." + key + " missing in /health");
        }
        return Long.parseLong(m.group(1));
    }

    static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isEmpty() ? fallback : v;
    }
}
