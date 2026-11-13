//! In-process read benchmark for the Databend POC engine.
//!
//! Bypasses the Python gateway, the stdin JSON RPC and Databend entirely: synthetic rows are
//! loaded straight into a ReadySet base table, caches are declared through the same native
//! pipeline as `databend-m2` (`Declaration` -> `native_query::compile` -> `create_cache`), and
//! concurrent tasks issue reads through `native_query::read_with`.
//!
//! `--mode local` mirrors the standalone ReadySet adapter: the server shares its `Readers` map
//! and reads use the in-process `ReadRequestHandler`. `--mode rpc` uses the View's TCP RPC,
//! exactly like `databend-m2` today. `--path sql` parses/rewrites/matches SQL text on every
//! request (what an HTTP text-query frontend has to do); `--path prepared` reuses precompiled
//! parameters to isolate the reader itself.
//!
//! The load generator runs in the same process and competes for the same CPUs. Results are an
//! upper bound for the engine read path, NOT end-to-end HTTP numbers.
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, ValueEnum};
use readyset_client::View;
use readyset_client::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use readyset_client::read::LocalReader;
use readyset_client::recipe::changelist::{Change, ChangeList};
use readyset_data::Dialect;
use readyset_databend::SourceRow;
use readyset_databend::declaration::{Declaration, Template};
use readyset_databend::native_query::{self, LOCAL_DDL, LOCAL_TABLE};
use readyset_server::worker::readers::{ReadRequestHandler, retry_misses};
use readyset_server::{Builder, DurabilityMode, Handle, PersistenceParameters};
use readyset_sql::ast::TrxCachePolicy;
use readyset_sql_passes::adapter_rewrites::DfQueryParameters;
use serde_json::{Value, json};

use readyset_alloc as _;

const DATABASE: &str = "poc";

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// Shared Readers + in-process ReadRequestHandler (standalone adapter path).
    Local,
    /// View TCP RPC to the reader listener (current databend-m2 path).
    Rpc,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Workload {
    /// `WHERE id = ?`, 1 row.
    Point,
    /// `WHERE id IN (?, ...)`, --in-keys lookups.
    In,
    /// `WHERE score BETWEEN ? AND ?`, --range-width * --score-group rows.
    Range,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum PathKind {
    /// Parse + native rewrite + slot matching per request, then read.
    Sql,
    /// Precompiled parameters (random pool), read only.
    Prepared,
}

#[derive(Parser, Clone)]
struct Args {
    #[arg(long, value_enum, default_value = "local")]
    mode: Mode,
    #[arg(long, default_value_t = 1_000_000)]
    rows: i64,
    /// Rows sharing one score value.
    #[arg(long, default_value_t = 10)]
    score_group: i64,
    #[arg(long, default_value_t = 10)]
    in_keys: usize,
    /// Distinct score values per range query.
    #[arg(long, default_value_t = 5)]
    range_width: i64,
    #[arg(long, value_delimiter = ',', default_value = "point,in,range")]
    workloads: Vec<Workload>,
    #[arg(long, value_delimiter = ',', default_value = "sql,prepared")]
    paths: Vec<PathKind>,
    #[arg(long, value_delimiter = ',', default_value = "1,8,32,128")]
    concurrency: Vec<usize>,
    #[arg(long, default_value_t = 10)]
    duration_secs: u64,
    #[arg(long, default_value_t = 2)]
    warmup_secs: u64,
    /// Precompiled parameter sets per workload for --path prepared.
    #[arg(long, default_value_t = 65_536)]
    pool: usize,
    /// Background inserts of NEW ids per second during every measured step (0 = read only).
    #[arg(long, default_value_t = 0)]
    write_rps: u64,
    /// Tokio worker threads (default: number of CPUs).
    #[arg(long)]
    worker_threads: Option<usize>,
    #[arg(long)]
    output: PathBuf,
}

/// SplitMix64: deterministic, dependency-free, plenty for key selection.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: i64) -> i64 {
        (self.next() % n as u64) as i64
    }
}

fn row(id: i64, group: i64) -> SourceRow {
    SourceRow {
        id,
        payload: format!("payload-{id:012}"),
        score: id / group,
        note: (id % 2 == 1).then(|| format!("note-{}", id % 100)),
    }
}

/// One declared workload: its native template, a request generator and an exact row count.
struct Spec {
    workload: Workload,
    template: Template,
    view_name: String,
}

struct Shape {
    rows: i64,
    group: i64,
    in_keys: usize,
    width: i64,
}

impl Shape {
    /// Returns read SQL and the exact expected row count for the synthetic data set.
    fn request(&self, workload: Workload, rng: &mut Rng) -> (String, usize) {
        let table = format!("{DATABASE}.source_rows");
        match workload {
            Workload::Point => (
                format!(
                    "SELECT id,payload,score,note FROM {table} WHERE id = {}",
                    rng.below(self.rows)
                ),
                1,
            ),
            Workload::In => {
                let mut keys: Vec<i64> = (0..self.in_keys).map(|_| rng.below(self.rows)).collect();
                let list = keys
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                keys.sort_unstable();
                keys.dedup();
                (
                    format!("SELECT id,payload FROM {table} WHERE id IN ({list})"),
                    keys.len(),
                )
            }
            Workload::Range => {
                // Only full score groups, so every range returns exactly width * group rows.
                let full = self.rows / self.group;
                let low = rng.below(full - self.width + 1);
                (
                    format!(
                        "SELECT id,payload FROM {table} WHERE score BETWEEN {low} AND {}",
                        low + self.width - 1
                    ),
                    (self.width * self.group) as usize,
                )
            }
        }
    }
}

fn template_sql(workload: Workload, in_keys: usize) -> String {
    let table = format!("{DATABASE}.source_rows");
    match workload {
        Workload::Point => format!("SELECT id,payload,score,note FROM {table} WHERE id = ?"),
        Workload::In => format!(
            "SELECT id,payload FROM {table} WHERE id IN ({})",
            vec!["?"; in_keys].join(",")
        ),
        Workload::Range => format!("SELECT id,payload FROM {table} WHERE score BETWEEN ? AND ?"),
    }
}

struct Ctx {
    declaration: Declaration,
    shape: Shape,
    specs: Vec<Spec>,
    views: Vec<View>,
    pools: Vec<Vec<(DfQueryParameters, usize)>>,
}

async fn run_read(
    view: &mut View,
    local: Option<&mut ReadRequestHandler>,
    template: &Template,
    params: &DfQueryParameters,
) -> Result<usize> {
    let local = local.map(|l| l as &mut dyn LocalReader);
    let (rows, _) = native_query::read_with(view, local, &template.query, params)
        .await?
        .context("local result budget miss")?;
    Ok(rows.len())
}

/// The full per-request work of a text-SQL frontend: parse, native rewrite, native slot
/// matching against the declared template, then the native read and JSON conversion.
async fn execute_sql(
    ctx: &Ctx,
    spec: &Spec,
    view: &mut View,
    local: Option<&mut ReadRequestHandler>,
    sql: &str,
) -> Result<usize> {
    let read = ctx
        .declaration
        .read_query(sql, DATABASE)
        .context("read SQL did not compile")?;
    let params = spec
        .template
        .query
        .read_params(&read)
        .context("read did not match the declared template")?;
    run_read(view, local, &spec.template, &params).await
}

#[derive(Default)]
struct TaskResult {
    samples: Vec<u32>,
    errors: u64,
    mismatches: u64,
    first_error: Option<String>,
}

struct Window {
    measure_start: Instant,
    end: Instant,
}

async fn worker(
    ctx: Arc<Ctx>,
    index: usize,
    path: PathKind,
    seed: u64,
    window: Arc<Window>,
    // One handler per task, like one per adapter connection: it is Send but not Sync.
    mut local: Option<ReadRequestHandler>,
) -> TaskResult {
    let spec = &ctx.specs[index];
    let pool = &ctx.pools[index];
    let mut view = ctx.views[index].clone();
    let mut rng = Rng(seed);
    let mut out = TaskResult::default();
    let mut iteration = 0u64;
    loop {
        iteration += 1;
        if iteration.is_multiple_of(64) {
            // In-process reads often complete without suspending; let timers and the
            // controller run on this worker thread too.
            tokio::task::yield_now().await;
        }
        let sql_request = match path {
            PathKind::Sql => Some(ctx.shape.request(spec.workload, &mut rng)),
            PathKind::Prepared => None,
        };
        let pick = rng.below(pool.len().max(1) as i64) as usize;
        let start = Instant::now();
        if start >= window.end {
            break;
        }
        let (result, expected) = match &sql_request {
            Some((sql, expected)) => (
                execute_sql(&ctx, spec, &mut view, local.as_mut(), sql).await,
                *expected,
            ),
            None => {
                let (params, expected) = &pool[pick];
                (
                    run_read(&mut view, local.as_mut(), &spec.template, params).await,
                    *expected,
                )
            }
        };
        let elapsed = start.elapsed();
        if start < window.measure_start {
            continue;
        }
        out.samples
            .push(u32::try_from(elapsed.as_nanos()).unwrap_or(u32::MAX));
        match result {
            Ok(n) if n == expected => {}
            Ok(_) => out.mismatches += 1,
            Err(error) => {
                out.errors += 1;
                out.first_error.get_or_insert_with(|| format!("{error:#}"));
            }
        }
    }
    out
}

fn percentile(sorted: &[u32], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    f64::from(sorted[rank.min(sorted.len() - 1)]) / 1000.0
}

fn latency_summary(mut samples: Vec<u32>) -> Value {
    samples.sort_unstable();
    let mean = if samples.is_empty() {
        0.0
    } else {
        samples.iter().map(|&s| f64::from(s)).sum::<f64>() / samples.len() as f64 / 1000.0
    };
    json!({
        "mean_us": round2(mean),
        "p50_us": round2(percentile(&samples, 50.0)),
        "p90_us": round2(percentile(&samples, 90.0)),
        "p99_us": round2(percentile(&samples, 99.0)),
        "p999_us": round2(percentile(&samples, 99.9)),
        "max_us": round2(samples.last().map_or(0.0, |&s| f64::from(s) / 1000.0)),
    })
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn rss_mib() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib / 1024)
}

/// Background inserts of fresh ids (never read by the measured workloads, so expected row
/// counts stay exact) plus a probe measuring insert-ack -> visible-in-reader latency.
struct Writes {
    table: tokio::sync::Mutex<readyset_client::Table>,
    next_id: AtomicI64,
    last_written: AtomicI64,
    written: AtomicU64,
    stop: AtomicBool,
    group: i64,
}

async fn writer(
    writes: Arc<Writes>,
    rps: u64,
    acked: Arc<std::sync::Mutex<BTreeMap<i64, Instant>>>,
) -> Result<()> {
    let per_tick = (rps / 100).max(1) as i64;
    let mut table = writes.table.lock().await;
    let mut ticker = tokio::time::interval(Duration::from_millis(10));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !writes.stop.load(Ordering::Relaxed) {
        ticker.tick().await;
        let first = writes.next_id.fetch_add(per_tick, Ordering::Relaxed);
        table
            .insert_many((first..first + per_tick).map(|id| row(id, writes.group).values()))
            .await?;
        let last = first + per_tick - 1;
        writes.last_written.store(last, Ordering::Release);
        writes.written.fetch_add(per_tick as u64, Ordering::Relaxed);
        if let Ok(mut acked) = acked.lock() {
            acked.insert(last, Instant::now());
        }
    }
    Ok(())
}

async fn probe(
    ctx: Arc<Ctx>,
    point: usize,
    writes: Arc<Writes>,
    acked: Arc<std::sync::Mutex<BTreeMap<i64, Instant>>>,
    mut local: Option<ReadRequestHandler>,
) -> Result<Vec<u32>> {
    let spec = &ctx.specs[point];
    let mut view = ctx.views[point].clone();
    let mut lags = Vec::new();
    while !writes.stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let id = writes.last_written.load(Ordering::Acquire);
        if id < 0 {
            continue;
        }
        let Some(ack) = acked.lock().ok().and_then(|m| m.get(&id).copied()) else {
            continue;
        };
        let sql =
            format!("SELECT id,payload,score,note FROM {DATABASE}.source_rows WHERE id = {id}");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if execute_sql(&ctx, spec, &mut view, local.as_mut(), &sql).await? == 1 {
                lags.push(u32::try_from(ack.elapsed().as_nanos()).unwrap_or(u32::MAX));
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "inserted row {id} not visible after 5s"
            );
            tokio::time::sleep(Duration::from_micros(200)).await;
        }
        if let Ok(mut acked) = acked.lock() {
            // Keep the map bounded: older acknowledgements are never probed again.
            *acked = acked.split_off(&id);
        }
    }
    Ok(lags)
}

async fn step(
    ctx: &Arc<Ctx>,
    local: Option<&ReadRequestHandler>,
    index: usize,
    path: PathKind,
    concurrency: usize,
    args: &Args,
    writes: Option<&Arc<Writes>>,
) -> Result<Value> {
    let now = Instant::now();
    let window = Arc::new(Window {
        measure_start: now + Duration::from_secs(args.warmup_secs),
        end: now + Duration::from_secs(args.warmup_secs + args.duration_secs),
    });
    let mut background = None;
    if let Some(writes) = writes {
        writes.stop.store(false, Ordering::Relaxed);
        let acked = Arc::new(std::sync::Mutex::new(BTreeMap::new()));
        let before = writes.written.load(Ordering::Relaxed);
        let w = tokio::spawn(writer(
            Arc::clone(writes),
            args.write_rps,
            Arc::clone(&acked),
        ));
        let point = ctx.specs.iter().position(|s| s.workload == Workload::Point);
        let p = point.map(|p| {
            tokio::spawn(probe(
                Arc::clone(ctx),
                p,
                Arc::clone(writes),
                acked,
                local.cloned(),
            ))
        });
        background = Some((w, p, before));
    }
    let tasks: Vec<_> = (0..concurrency)
        .map(|t| {
            let seed = 0x5EED ^ ((index as u64) << 48) ^ ((t as u64) << 16) ^ concurrency as u64;
            tokio::spawn(worker(
                Arc::clone(ctx),
                index,
                path,
                seed,
                Arc::clone(&window),
                local.cloned(),
            ))
        })
        .collect();
    let mut merged = TaskResult::default();
    for task in tasks {
        let r = task.await?;
        merged.samples.extend(r.samples);
        merged.errors += r.errors;
        merged.mismatches += r.mismatches;
        if merged.first_error.is_none() {
            merged.first_error = r.first_error;
        }
    }
    let mut write_report = Value::Null;
    if let (Some(writes), Some((w, p, before))) = (writes, background) {
        writes.stop.store(true, Ordering::Relaxed);
        w.await??;
        let written = writes.written.load(Ordering::Relaxed) - before;
        let lag = match p {
            Some(p) => latency_summary(p.await??),
            None => Value::Null,
        };
        let elapsed = (Instant::now() - now).as_secs_f64();
        write_report = json!({"achieved_rps": round2(written as f64 / elapsed),
            "visibility_lag": lag, "lag_resolution": "~1ms timer + 20ms probe interval"});
    }
    let count = merged.samples.len();
    let qps = count as f64 / args.duration_secs as f64;
    let latency = latency_summary(merged.samples);
    let spec = &ctx.specs[index];
    let report = json!({
        "workload": format!("{:?}", spec.workload).to_lowercase(),
        "path": format!("{path:?}").to_lowercase(),
        "concurrency": concurrency,
        "requests": count,
        "qps": round2(qps),
        "latency": latency,
        "errors": merged.errors,
        "mismatches": merged.mismatches,
        "first_error": merged.first_error,
        "writes": write_report,
    });
    println!(
        "{:<6} {:<8} c={:<4} qps={:>11.0} p50={:>8.1}us p99={:>8.1}us p999={:>9.1}us err={} mismatch={}{}",
        report["workload"].as_str().unwrap_or(""),
        report["path"].as_str().unwrap_or(""),
        concurrency,
        qps,
        report["latency"]["p50_us"].as_f64().unwrap_or(0.0),
        report["latency"]["p99_us"].as_f64().unwrap_or(0.0),
        report["latency"]["p999_us"].as_f64().unwrap_or(0.0),
        merged.errors,
        merged.mismatches,
        if report["writes"].is_null() {
            String::new()
        } else {
            format!(
                " writes={} lag_p50={}us lag_p99={}us",
                report["writes"]["achieved_rps"],
                report["writes"]["visibility_lag"]["p50_us"],
                report["writes"]["visibility_lag"]["p99_us"]
            )
        }
    );
    Ok(report)
}

async fn wait_converged(view: &View, expected: usize, label: &str) -> Result<()> {
    let mut reader = view
        .clone()
        .into_reader_handle()
        .context("single reader required")?;
    let deadline = Instant::now() + Duration::from_secs(900);
    loop {
        let len = reader.len().await?;
        if len == expected {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "{label}: reader has {len} keys, expected {expected}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn run(args: &Args) -> Result<Value> {
    let mut builder = Builder::default();
    builder.set_external_addr("127.0.0.1:0".parse()?);
    builder.set_dialect(readyset_sql::Dialect::MySQL);
    builder.disable_partial();
    builder.set_mixed_comparisons(true);
    builder.set_persistence(PersistenceParameters {
        mode: DurabilityMode::DeleteOnExit,
        db_filename_prefix: "bench".into(),
        storage_dir: Some(args.output.join("storage")),
        wal_flush_interval_seconds: 1,
        ..Default::default()
    });
    let readers = Default::default();
    let (mut handle, shutdown): (Handle, _) = match args.mode {
        // The standalone ReadySet binary passes this same fixed address: reads never use it.
        Mode::Local => {
            let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(Arc::new(
                LocalAuthorityStore::new(),
            ))));
            builder
                .start_with_readers(authority, Arc::clone(&readers), "127.0.0.1:4000".parse()?)
                .await?
        }
        Mode::Rpc => builder.start_local().await?,
    };
    let local = (args.mode == Mode::Local).then(|| {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(retry_misses(rx));
        ReadRequestHandler::new(Arc::clone(&readers), tx, Duration::from_secs(5))
    });
    let outcome = measure(args, &mut handle, local).await;
    drop(handle);
    shutdown.shutdown_timeout(Duration::from_secs(30)).await;
    outcome
}

async fn measure(
    args: &Args,
    handle: &mut Handle,
    local: Option<ReadRequestHandler>,
) -> Result<Value> {
    handle.backend_ready().await;
    handle
        .extend_recipe(ChangeList::from_strings(
            vec![LOCAL_DDL],
            Dialect::DEFAULT_MYSQL,
        )?)
        .await?;
    let load_start = Instant::now();
    let mut table = handle.table(LOCAL_TABLE).await?;
    let mut next = 0;
    while next < args.rows {
        let end = (next + 10_000).min(args.rows);
        table
            .insert_many((next..end).map(|id| row(id, args.score_group).values()))
            .await?;
        next = end;
    }
    let load_secs = load_start.elapsed().as_secs_f64();
    eprintln!("loaded {} rows in {load_secs:.1}s", args.rows);

    let declaration = Declaration {
        database: DATABASE.into(),
        table: "source_rows".into(),
        queries: vec![],
    };
    let shape = Shape {
        rows: args.rows,
        group: args.score_group,
        in_keys: args.in_keys,
        width: args.range_width,
    };
    let mut workloads = args.workloads.clone();
    workloads.dedup();
    let cache_start = Instant::now();
    let mut specs = Vec::new();
    let mut views = Vec::new();
    for workload in workloads {
        let name = format!("bench_{workload:?}").to_lowercase();
        let template = declaration.template_for_sql(
            name.clone(),
            &template_sql(workload, args.in_keys),
            DATABASE,
            LOCAL_TABLE,
        )?;
        handle
            .extend_recipe(ChangeList::from_change(
                Change::create_cache(
                    name.clone(),
                    template.query.statement.clone(),
                    TrxCachePolicy::Never,
                    None,
                    None,
                ),
                Dialect::DEFAULT_MYSQL,
            ))
            .await?;
        let view = handle.view(name.as_str()).await?;
        ensure!(matches!(view, View::Single(_)), "single reader required");
        let keys = match workload {
            Workload::Point | Workload::In => args.rows as usize,
            Workload::Range => ((args.rows + args.score_group - 1) / args.score_group) as usize,
        };
        // Migration returns only after QueryReplayDone (full replay). The RPC reader size is
        // checked too where a reader listener exists; in local mode there is none (same as the
        // standalone adapter), and every measured request checks its exact row count instead.
        if args.mode == Mode::Rpc {
            wait_converged(&view, keys, &name).await?;
        }
        specs.push(Spec {
            workload,
            template,
            view_name: name,
        });
        views.push(view);
    }
    ensure!(
        handle
            .materialization_info()
            .await?
            .iter()
            .all(|m| !m.partial),
        "partial query state"
    );
    let cache_secs = cache_start.elapsed().as_secs_f64();
    eprintln!(
        "caches materialized in {cache_secs:.1}s, rss={:?} MiB",
        rss_mib()
    );

    // Precompile parameter pools (also proves every generated request matches natively).
    let mut rng = Rng(0xC0FFEE);
    let mut pools = Vec::new();
    for spec in &specs {
        let size = if args.paths.contains(&PathKind::Prepared) {
            args.pool
        } else {
            1
        };
        let mut pool = Vec::with_capacity(size);
        for _ in 0..size {
            let (sql, expected) = shape.request(spec.workload, &mut rng);
            let read = declaration
                .read_query(&sql, DATABASE)
                .with_context(|| format!("did not compile: {sql}"))?;
            let params = spec
                .template
                .query
                .read_params(&read)
                .with_context(|| format!("did not match {}: {sql}", spec.view_name))?;
            pool.push((params, expected));
        }
        pools.push(pool);
    }
    let ctx = Arc::new(Ctx {
        declaration,
        shape,
        specs,
        views,
        pools,
    });

    // Value-level correctness sample before timing anything.
    let point = ctx.specs.iter().position(|s| s.workload == Workload::Point);
    if let Some(point) = point {
        let mut view = ctx.views[point].clone();
        let mut local = local.clone();
        for _ in 0..1000 {
            let id = rng.below(args.rows);
            let read = ctx
                .declaration
                .read_query(
                    &format!(
                        "SELECT id,payload,score,note FROM {DATABASE}.source_rows WHERE id = {id}"
                    ),
                    DATABASE,
                )
                .context("point read")?;
            let params = ctx.specs[point]
                .template
                .query
                .read_params(&read)
                .context("point match")?;
            let local = local.as_mut().map(|l| l as &mut dyn LocalReader);
            let (rows, _) = native_query::read_with(
                &mut view,
                local,
                &ctx.specs[point].template.query,
                &params,
            )
            .await?
            .context("budget")?;
            let r = row(id, args.score_group);
            let expected = vec![vec![
                json!(r.id.to_string()),
                json!(r.payload),
                json!(r.score.to_string()),
                r.note.map_or(Value::Null, Value::String),
            ]];
            if rows != expected {
                bail!("value mismatch for id {id}: {rows:?}");
            }
        }
    }

    let writes = if args.write_rps > 0 {
        Some(Arc::new(Writes {
            table: tokio::sync::Mutex::new(handle.table(LOCAL_TABLE).await?),
            next_id: AtomicI64::new(args.rows),
            last_written: AtomicI64::new(-1),
            written: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            group: args.score_group,
        }))
    } else {
        None
    };
    let mut steps = Vec::new();
    for index in 0..ctx.specs.len() {
        for &path in &args.paths {
            for &concurrency in &args.concurrency {
                steps.push(
                    step(
                        &ctx,
                        local.as_ref(),
                        index,
                        path,
                        concurrency,
                        args,
                        writes.as_ref(),
                    )
                    .await?,
                );
            }
        }
    }
    Ok(json!({
        "passed": steps.iter().all(|s| s["errors"] == 0 && s["mismatches"] == 0),
        "mode": format!("{:?}", args.mode).to_lowercase(),
        "rows": args.rows,
        "score_group": args.score_group,
        "in_keys": args.in_keys,
        "range_rows": args.range_width * args.score_group,
        "duration_secs": args.duration_secs,
        "warmup_secs": args.warmup_secs,
        "write_rps_target": args.write_rps,
        "worker_threads": args.worker_threads,
        "cpus": std::thread::available_parallelism().map(|n| n.get()).ok(),
        "load_secs": round2(load_secs),
        "cache_secs": round2(cache_secs),
        "rss_mib_after_load": rss_mib(),
        "rss_mib_end": rss_mib(),
        "scope": "in-process engine read path; load generator shares CPUs; no HTTP, no Databend",
        "steps": steps,
    }))
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    ensure!(!args.output.exists(), "output directory must not exist");
    ensure!(
        args.rows > 0 && args.score_group > 0,
        "rows and score-group must be positive"
    );
    ensure!(
        args.range_width > 0 && args.rows / args.score_group >= args.range_width,
        "range width must fit within full score groups"
    );
    ensure!(
        (1..=1000).contains(&args.in_keys),
        "in-keys must be 1..=1000"
    );
    ensure!(
        !args.concurrency.is_empty() && args.concurrency.iter().all(|&c| c > 0),
        "concurrency must be positive"
    );
    ensure!(
        args.duration_secs > 0 && args.pool > 0,
        "duration and pool must be positive"
    );
    fs::create_dir_all(&args.output)?;
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.enable_all();
    if let Some(threads) = args.worker_threads {
        runtime.worker_threads(threads);
    }
    let report = runtime.build()?.block_on(run(&args));
    let value = match &report {
        Ok(v) => v.clone(),
        Err(error) => json!({"passed": false, "error": format!("{error:#}")}),
    };
    fs::write(
        args.output.join("report.json"),
        serde_json::to_vec_pretty(&value)?,
    )?;
    println!("Report: {}", args.output.join("report.json").display());
    report.map(|_| ())
}
