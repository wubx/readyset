//! M1: static full-table import and point lookups, using the real ReadySet engine.
//! Databend export is a separate bounded Python step; this executable has no source connection.
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use database_utils::UpstreamConfig;
use dataflow_state::{DurabilityMode, PersistenceParameters};
use readyset_client::recipe::ChangeList;
use readyset_client::{ReaderHandle, ReadySetHandle};
use readyset_data::{DfValue, Dialect};
use readyset_server::{Builder, Handle};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::task::JoinSet;

// Link the same allocator used by ReadySet. The server's allocator statistics depend on it.
use readyset_alloc as _;

const TABLE: &str = "m1_source";
const QUERY: &str = "m1_by_id";

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    input: PathBuf,
    /// Must not exist. Each experiment uses an independent ReadySet instance and storage path.
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 1000)]
    batch_size: usize,
    #[arg(long, default_value_t = 10000)]
    repeat_queries: usize,
    #[arg(long, default_value_t = 1000)]
    missing_queries: usize,
    #[arg(long, default_value_t = 16)]
    concurrency: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

/// Export contract: JSON integers (not floats), UTF-8 text and nullable UTF-8 text.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: i64,
    payload: String,
    score: i64,
    note: Option<String>,
}

impl Row {
    fn values(&self) -> Vec<DfValue> {
        vec![
            self.id.into(),
            self.payload.as_str().into(),
            self.score.into(),
            self.note
                .as_deref()
                .map(DfValue::from)
                .unwrap_or(DfValue::None),
        ]
    }
}

fn read_rows(path: &Path) -> Result<Vec<Row>> {
    let mut ids = HashSet::new();
    let mut rows = Vec::new();
    for (index, line) in BufReader::new(File::open(path)?).lines().enumerate() {
        let row: Row =
            serde_json::from_str(&line?).with_context(|| format!("input line {}", index + 1))?;
        ensure!(ids.insert(row.id), "duplicate business key {}", row.id);
        rows.push(row);
    }
    ensure!(
        !rows.is_empty(),
        "empty input: M1 requires at least one row"
    );
    Ok(rows)
}

// Reproducible Fisher-Yates permutation with rejection sampling, without coupling to rand versions.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, bound: usize) -> usize {
        let bound = bound as u64;
        let limit = u64::MAX - u64::MAX % bound;
        loop {
            let n = self.next();
            if n < limit {
                return (n % bound) as usize;
            }
        }
    }
    fn shuffle(&mut self, values: &mut [usize]) {
        for i in (1..values.len()).rev() {
            values.swap(i, self.below(i + 1));
        }
    }
}

#[derive(Debug, Serialize)]
struct Summary {
    queries: usize,
    wall_seconds: f64,
    qps: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    max_us: f64,
}

fn summary(mut micros: Vec<f64>, elapsed: Duration) -> Result<Summary> {
    ensure!(!micros.is_empty(), "no latency samples");
    micros.sort_by(f64::total_cmp);
    let percentile = |p: f64| micros[((micros.len() as f64 * p).ceil() as usize).saturating_sub(1)];
    Ok(Summary {
        queries: micros.len(),
        wall_seconds: elapsed.as_secs_f64(),
        qps: micros.len() as f64 / elapsed.as_secs_f64(),
        p50_us: percentile(0.50),
        p95_us: percentile(0.95),
        p99_us: percentile(0.99),
        max_us: micros[micros.len() - 1],
    })
}

async fn lookup(reader: &mut ReaderHandle, row: Option<&Row>, id: i64) -> Result<f64> {
    let start = Instant::now();
    let actual = tokio::time::timeout(
        Duration::from_secs(10),
        reader.lookup(&[id.into()], Dialect::DEFAULT_MYSQL),
    )
    .await
    .context("point lookup timed out")??
    .into_vec();
    let latency = start.elapsed().as_secs_f64() * 1e6;
    let expected = row.map(|r| vec![r.values()]).unwrap_or_default();
    ensure!(
        actual == expected,
        "wrong result for key {id}: actual={actual:?}, expected={expected:?}"
    );
    Ok(latency)
}

fn rss_kib() -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|line| line.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

async fn wait_full(reader: &mut ReaderHandle, expected: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let actual = reader.len().await?;
            ensure!(
                actual <= expected,
                "reader has unexpected rows: {actual} > {expected}"
            );
            if actual == expected {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("full materialization did not become complete")?
}

async fn run_engine(args: &Args, rows: Arc<Vec<Row>>, handle: &mut Handle) -> Result<Value> {
    let started = Instant::now();
    handle.extend_recipe(ChangeList::from_strings(
        vec![format!("CREATE TABLE {TABLE} (id BIGINT NOT NULL, payload TEXT NOT NULL, score BIGINT NOT NULL, note TEXT, PRIMARY KEY (id))")],
        Dialect::DEFAULT_MYSQL,
    )?).await?;
    let mut table = handle.table(TABLE).await?;
    // M1 deliberately uses ordinary persistent writes. Snapshot mode and crash-resume are M2 work.
    let load_started = Instant::now();
    for chunk in rows.chunks(args.batch_size) {
        table.insert_many(chunk.iter().map(Row::values)).await?;
    }
    let submitted_seconds = load_started.elapsed().as_secs_f64();
    // Create the cache AFTER submitting all writes: its full replay must cover the entire base.
    let index_started = Instant::now();
    handle.extend_recipe(ChangeList::from_strings(
        vec![format!("CREATE CACHE {QUERY} FROM SELECT id, payload, score, note FROM {TABLE} WHERE id = ?")],
        Dialect::DEFAULT_MYSQL,
    )?).await?;
    let mut reader = handle
        .view(QUERY)
        .await?
        .into_reader_handle()
        .context("single-reader view required")?;
    wait_full(&mut reader, rows.len()).await?;
    let ready_seconds = index_started.elapsed().as_secs_f64();
    println!(
        "Full reader ready with {} rows before first lookup",
        rows.len()
    );
    // Existing controller introspection verifies no materialization in this graph is partial.
    let mats = handle.materialization_info().await?;
    ensure!(!mats.is_empty(), "missing materialization introspection");
    ensure!(
        mats.iter().all(|m| !m.partial),
        "partial materialization would invalidate cold-key coverage"
    );
    fs::write(
        args.output.join("materializations.json"),
        serde_json::to_vec_pretty(&mats)?,
    )?;
    let mut rng = Rng(args.seed);
    let mut order: Vec<usize> = (0..rows.len()).collect();
    rng.shuffle(&mut order);
    let first_key = rows[order[0]].id;
    let start = Instant::now();
    let mut times = Vec::with_capacity(order.len());
    for &i in &order {
        times.push(lookup(&mut reader, Some(&rows[i]), rows[i].id).await?);
    }
    let first_request_us = times[0];
    fs::write(
        args.output.join("first_touch_us.json"),
        serde_json::to_vec(&times)?,
    )?;
    let first = summary(times, start.elapsed())?;
    println!("First-touch random permutation (all keys): {first:?}");
    let start = Instant::now();
    let mut times = Vec::with_capacity(args.repeat_queries);
    for _ in 0..args.repeat_queries {
        let row = &rows[rng.below(rows.len())];
        times.push(lookup(&mut reader, Some(row), row.id).await?);
    }
    let repeated = summary(times, start.elapsed())?;
    let known: HashSet<i64> = rows.iter().map(|r| r.id).collect();
    let mut candidate = i64::MIN;
    let start = Instant::now();
    let mut times = Vec::with_capacity(args.missing_queries);
    for _ in 0..args.missing_queries {
        while known.contains(&candidate) {
            candidate = candidate.checked_add(1).context("missing key overflow")?;
        }
        times.push(lookup(&mut reader, None, candidate).await?);
        candidate = candidate.checked_add(1).context("missing key overflow")?;
    }
    let missing = summary(times, start.elapsed())?;
    // Independent reader handles per task; no single shared mutex serializing requests.
    let mut readers = Vec::new();
    for _ in 0..args.concurrency {
        readers.push(
            handle
                .view(QUERY)
                .await?
                .into_reader_handle()
                .context("reader")?,
        );
    }
    let start = Instant::now();
    let mut tasks = JoinSet::new();
    for (worker, mut reader) in readers.into_iter().enumerate() {
        let rows = rows.clone();
        let count = args.repeat_queries / args.concurrency
            + usize::from(worker < args.repeat_queries % args.concurrency);
        let seed = rng.next();
        tasks.spawn(async move {
            let mut rng = Rng(seed);
            let mut times = Vec::with_capacity(count);
            for _ in 0..count {
                let row = &rows[rng.below(rows.len())];
                times.push(lookup(&mut reader, Some(row), row.id).await?);
            }
            Ok::<_, anyhow::Error>(times)
        });
    }
    let mut times = Vec::new();
    while let Some(result) = tasks.join_next().await {
        times.extend(result??);
    }
    let concurrent = summary(times, start.elapsed())?;
    ensure!(
        reader.len().await? == rows.len(),
        "reader row count changed"
    );
    let mut controller: ReadySetHandle = handle.c.as_ref().context("controller")?.clone();
    let final_mats = controller.materialization_info().await?;
    ensure!(
        final_mats.iter().all(|m| !m.partial),
        "partial state appeared"
    );
    Ok(json!({
        "passed": true, "build": if cfg!(debug_assertions) { "debug_assertions_enabled" } else { "optimized" },
        "rows_loaded": rows.len(), "rows_verified": first.queries,
        "query": format!("SELECT id, payload, score, note FROM {TABLE} WHERE id = ?"),
        "partial_materialization": false, "reader_rows_before_first_lookup": rows.len(),
        "persistent_base": true, "full_materialization_persistence": false,
        "source_connection_during_lookup": false,
        "topology": "in-process ReadySet server plus TCP ReaderHandle RPC on loopback; no SQL wire frontend",
        "seed": args.seed, "batch_size": args.batch_size, "concurrency": args.concurrency,
        "load_submit_seconds": submitted_seconds, "index_ready_seconds": ready_seconds,
        "total_engine_seconds": started.elapsed().as_secs_f64(),
        "first_query_key": first_key, "first_request_us": first_request_us,
        "first_touch_all_keys": first, "repeated_uniform": repeated,
        "missing_keys": missing, "concurrent_uniform": concurrent,
        "process_rss_kib": rss_kib(), "materializations": final_mats,
        "limitations": ["No SQL protocol or remote network", "No restart/crash recovery test", "No CDC applied", "No production-scale capacity claim"],
    }))
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    ensure!(
        args.batch_size > 0
            && args.repeat_queries > 0
            && args.missing_queries > 0
            && args.concurrency > 0,
        "batch size, query counts and concurrency must be nonzero"
    );
    if args.output.exists() {
        bail!("output already exists: {}", args.output.display());
    }
    let rows = Arc::new(read_rows(&args.input)?);
    fs::create_dir_all(&args.output)?;
    let mut builder = Builder::default();
    builder.set_external_addr("127.0.0.1:0".parse()?);
    builder.set_dialect(readyset_sql::Dialect::MySQL);
    builder.disable_partial();
    // No upstream/CDC URL means no built-in replication task is started. Do not set
    // replication_enabled=false: that switch denotes shallow-only mode and wipes deep state
    // on leader election in this branch, which is unsuitable for an external data loader.
    builder.set_replicator_config(UpstreamConfig::default());
    builder.set_view_request_timeout(Duration::from_secs(10));
    builder.set_persistence(PersistenceParameters {
        mode: DurabilityMode::Permanent,
        db_filename_prefix: "m1".into(),
        storage_dir: Some(args.output.join("storage")),
        ..Default::default()
    });
    let (mut handle, shutdown) = builder.start_local().await?;
    let outcome = tokio::time::timeout(Duration::from_secs(600), async {
        handle.backend_ready().await;
        run_engine(&args, rows, &mut handle).await
    })
    .await
    .context("M1 exceeded 600 seconds")
    .and_then(|r| r);
    let report = match &outcome {
        Ok(report) => report.clone(),
        Err(error) => json!({"passed": false, "error": format!("{error:#}")}),
    };
    fs::write(
        args.output.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("Report: {}", args.output.join("report.json").display());
    drop(handle);
    shutdown.shutdown_timeout(Duration::from_secs(30)).await;
    outcome?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deterministic_shuffle_is_full_permutation() {
        let mut a: Vec<usize> = (0..1000).collect();
        let mut b = a.clone();
        Rng(42).shuffle(&mut a);
        Rng(42).shuffle(&mut b);
        assert_eq!(a, b);
        assert_ne!(a, (0..1000).collect::<Vec<_>>());
        a.sort();
        assert_eq!(a, (0..1000).collect::<Vec<_>>());
    }
    #[test]
    fn percentiles_use_nearest_rank() {
        let s = summary(vec![3.0, 1.0, 2.0, 4.0], Duration::from_secs(1)).unwrap();
        assert_eq!(s.p50_us, 2.0);
        assert_eq!(s.p99_us, 4.0);
        assert_eq!(s.qps, 4.0);
    }
    #[test]
    fn null_and_utf8_survive_mapping() {
        let r: Row =
            serde_json::from_str(r#"{"id":1,"payload":"中文é","score":-3,"note":null}"#).unwrap();
        assert_eq!(
            r.values(),
            vec![1.into(), "中文é".into(), (-3).into(), DfValue::None]
        );
    }
    #[test]
    fn duplicate_keys_rejected_before_engine_start() {
        let p = std::env::temp_dir().join(format!("m1-duplicates-{}.jsonl", std::process::id()));
        fs::write(
            &p,
            "{\"id\":1,\"payload\":\"a\",\"score\":0,\"note\":null}\n".repeat(2),
        )
        .unwrap();
        assert!(read_rows(&p).unwrap_err().to_string().contains("duplicate"));
        fs::remove_file(p).unwrap();
    }
}
