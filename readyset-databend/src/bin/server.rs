//! `readyset-databend`: single-binary Databend HTTP front end, in-process ReadySet engine
//! and Stream change capture (phase 1 of DESIGN.md). Replaces `poc/gateway.py` plus the
//! `databend-m2` child process; HTTP behaviour follows the gateway so the existing black-box
//! verifiers apply unchanged.
//!
//! The only Databend object is one Stream per synchronized table (next to the table);
//! tables, cache declarations and progress live in ReadySet. Scope is still one fixed-schema
//! table: on start the engine is rebuilt from a snapshot read AT this instance's Stream offset,
//! then the Stream's pending changes are consumed (the Stream is created on first start).
use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::response::Response;
use clap::Parser;
use readyset_databend::declaration::{Declaration, identifier};
use readyset_databend::engine::{BatchWriter, CacheConfig, Engine, EngineConfig, ReadOutcome};
use readyset_databend::replicator::{Names, Replicator, parse_changes};
use readyset_databend::upstream::{
    DatabendClient, Journal, MAX_RESPONSE, Pages, basic_auth, bounded_body, http_client,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::sync::watch;

use readyset_alloc as _;

const MAX_BODY: usize = 1024 * 1024;
/// Driver session endpoints forwarded as-is (see `App::session_endpoint`). `/v1/session/refresh`
/// is not among them: it needs a Databend session token, which the gateway never hands out.
const SESSION_PATHS: [&str; 3] = [
    "/v1/session/login",
    "/v1/session/heartbeat",
    "/v1/session/logout",
];
const MAX_LOCAL_QUERY_BYTES: usize = 8 * 1024 * 1024;
/// Rows per page of a local result, whatever page size the client asks for.
const MAX_LOCAL_PAGE_ROWS: usize = 10_000;
const MAX_LOCAL_TOTAL_BYTES: usize = 64 * 1024 * 1024;
const CONTROL_RESPONSE_RESERVE: usize = 2 * 1024 * 1024;
const MAX_LOCAL_RESULTS: usize = 2048;
const MAX_INFLIGHT: usize = 2048;
const LOCAL_RESULT_TTL: Duration = Duration::from_secs(60);
/// Client-chosen query IDs (x-databend-query-id) remembered after a local /final or /kill,
/// so a repeated lifecycle call never reaches Databend and the ID is not reused locally.
/// Generated `rs_local_` IDs need no memory: their prefix alone keeps them local.
const MAX_FINALIZED: usize = 65_536;
/// Bounds of `Drained` (fully delivered local results kept for a late lifecycle call).
const MAX_DRAINED: usize = 65_536;
const MAX_DRAINED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Parser)]
#[command(about = "Databend HTTP front end with an in-process ReadySet replica (phase 1)")]
struct Cli {
    /// TOML configuration file. Command-line flags override it.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Databend HTTP origin, e.g. http://databend:8000
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long)]
    user: Option<String>,
    /// Source database (single table in phase 1).
    #[arg(long)]
    database: Option<String>,
    #[arg(long)]
    table: Option<String>,
    /// Legacy gateway JSON: {"database","table","queries":[{"name","sql"}]}.
    #[arg(long)]
    declarations: Option<PathBuf>,
    /// Run directory for this process; must not exist.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Listen address, e.g. 0.0.0.0:8001
    #[arg(long)]
    listen: Option<String>,
    /// Loopback port (gateway-compatible shorthand for --listen 127.0.0.1:PORT).
    #[arg(long)]
    port: Option<u16>,
    /// Capture interval in seconds.
    #[arg(long)]
    interval: Option<f64>,
    #[arg(long)]
    max_staleness: Option<f64>,
    /// Explicitly allow creating this instance's Stream on the source table.
    #[arg(long)]
    confirm_create: bool,
    /// Accepted and ignored: the engine is in-process now.
    #[arg(long)]
    binary: Option<String>,
    /// Record route + header/session key NAMES per request in routes.jsonl.
    #[arg(long)]
    route_journal: bool,
    /// Test only: POST /__fault/engine fails the engine closed.
    #[arg(long)]
    enable_fault_injection: bool,
    /// POC regression only: keep every source row and re-verify the whole replica after each
    /// batch (at most 9,000 rows with unique ids). Default is production mode.
    #[arg(long)]
    oracle: bool,
    /// Use native partial materialization (cold keys replay from the local disk base).
    #[arg(long)]
    partial: bool,
    /// Allocated-memory soft target in MiB, requires --partial (not an RSS hard cap).
    #[arg(long)]
    memory_limit_mb: Option<usize>,
    /// Wait for native cache backfill; timeout forwards this read, never poisons the engine.
    #[arg(long)]
    upquery_timeout_ms: Option<u64>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    instance_id: Option<String>,
    confirm_create: Option<bool>,
    upstream: Option<UpstreamSection>,
    listen: Option<ListenSection>,
    replication: Option<ReplicationSection>,
    storage: Option<StorageSection>,
    cache: Option<CacheSection>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CacheSection {
    partial: Option<bool>,
    memory_limit_mb: Option<usize>,
    upquery_timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpstreamSection {
    url: Option<String>,
    user: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListenSection {
    http: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicationSection {
    tables: Option<Vec<String>>,
    poll_interval: Option<String>,
    max_staleness: Option<String>,
    /// Shared runtime: first delay before a failed table recovers itself (doubles, max 5m).
    retry_backoff: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageSection {
    dir: Option<PathBuf>,
}

struct Settings {
    /// TOML tables select the shared native runtime; CLI/oracle keeps its legacy contract.
    tables: Vec<(String, String)>,
    endpoint: String,
    user: String,
    password: String,
    declaration: Declaration,
    run_dir: PathBuf,
    /// Shared runtime only: stable native storage reopened on every restart.
    engine_dir: PathBuf,
    instance_id: String,
    listen: SocketAddr,
    interval: Duration,
    max_staleness: Duration,
    retry_backoff: Duration,
    route_journal: bool,
    fault_injection: bool,
    oracle: bool,
    cache: CacheConfig,
}

fn duration(text: &str) -> Result<Duration> {
    let text = text.trim();
    let (number, scale) = if let Some(ms) = text.strip_suffix("ms") {
        (ms, 0.001)
    } else if let Some(s) = text.strip_suffix('s') {
        (s, 1.0)
    } else {
        (text, 1.0)
    };
    let secs = number.trim().parse::<f64>()? * scale;
    ensure!(secs > 0.0 && secs <= 60.0, "duration must be in (0, 60s]");
    Ok(Duration::from_secs_f64(secs))
}

fn new_id(len: usize) -> String {
    uuid::Uuid::new_v4().simple().to_string()[..len].to_owned()
}

/// Instance identity survives restarts of the same storage directory (cluster prerequisite).
fn instance_id(configured: Option<String>, storage: Option<&Path>) -> Result<String> {
    let valid = |id: &str| {
        !id.is_empty()
            && id.len() <= 32
            && id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    };
    if let Some(id) = configured.filter(|id| !id.is_empty()) {
        ensure!(
            valid(&id),
            "instance_id must be 1..32 lowercase letters/digits"
        );
        return Ok(id);
    }
    let Some(storage) = storage else {
        return Ok(new_id(12));
    };
    let path = storage.join("instance_id");
    if path.exists() {
        let id = fs::read_to_string(&path)?.trim().to_owned();
        ensure!(valid(&id), "corrupt instance_id file");
        return Ok(id);
    }
    let id = new_id(12);
    fs::write(&path, format!("{id}\n"))?;
    Ok(id)
}

fn settings(cli: Cli) -> Result<Settings> {
    let file: FileConfig = match &cli.config {
        Some(path) => toml::from_str(&fs::read_to_string(path)?)
            .with_context(|| format!("invalid config {}", path.display()))?,
        None => FileConfig::default(),
    };
    let upstream = file.upstream.unwrap_or(UpstreamSection {
        url: None,
        user: None,
    });
    let replication = file.replication.unwrap_or(ReplicationSection {
        tables: None,
        poll_interval: None,
        max_staleness: None,
        retry_backoff: None,
    });
    let endpoint = cli
        .endpoint
        .or(upstream.url)
        .context("--endpoint or [upstream].url required")?;
    url_ok(&endpoint)?;
    let user = cli.user.or(upstream.user).unwrap_or_else(|| "root".into());
    ensure!(
        cli.confirm_create || file.confirm_create == Some(true),
        "requires --confirm-create (or confirm_create = true) to create tracking objects"
    );
    let mut declaration: Declaration = match &cli.declarations {
        Some(path) => serde_json::from_str(&fs::read_to_string(path)?)?,
        None => Declaration {
            database: String::new(),
            table: String::new(),
            queries: vec![],
        },
    };
    let shared_tables =
        if cli.database.is_none() && cli.table.is_none() && cli.declarations.is_none() {
            replication.tables.clone().unwrap_or_default()
        } else {
            ensure!(
                replication.tables.as_ref().is_none_or(Vec::is_empty),
                "use replication.tables OR CLI/declarations, not both"
            );
            vec![]
        };
    ensure!(
        shared_tables.len() <= 32,
        "at most 32 configured source tables"
    );
    ensure!(
        shared_tables.is_empty() || !cli.oracle,
        "oracle remains single-table CLI only"
    );
    let mut tables: Vec<(String, String)> = Vec::new();
    for name in &shared_tables {
        let (database, table) = name
            .split_once('.')
            .context("replication.tables entries must be db.table")?;
        ensure!(
            identifier(database) && identifier(table),
            "invalid source identifier"
        );
        let source = (database.to_owned(), table.to_owned());
        ensure!(
            !tables.iter().any(
                |(d, t)| d.eq_ignore_ascii_case(&source.0) && t.eq_ignore_ascii_case(&source.1)
            ),
            "duplicate configured source table (including case-folded identifiers)"
        );
        tables.push(source);
    }
    if declaration.database.is_empty() {
        let (database, table) = match (cli.database, cli.table) {
            (Some(d), Some(t)) => (d, t),
            (None, None) => {
                let tables = replication.tables.clone().unwrap_or_default();
                ensure!(
                    !tables.is_empty(),
                    "configure at least one replication.tables source"
                );
                let (d, t) = tables[0]
                    .split_once('.')
                    .context("replication.tables entries must be db.table")?;
                (d.to_owned(), t.to_owned())
            }
            _ => bail!("--database and --table must be given together"),
        };
        declaration.database = database;
        declaration.table = table;
    } else {
        ensure!(
            cli.database.is_none() && cli.table.is_none(),
            "use --declarations OR --database/--table"
        );
    }
    ensure!(
        identifier(&declaration.database) && identifier(&declaration.table),
        "invalid source identifier"
    );
    // Validates names, count and every bootstrap template through the native pipeline.
    declaration.templates(readyset_databend::native_query::LOCAL_TABLE)?;
    let storage = file.storage.and_then(|s| s.dir);
    let shared = !tables.is_empty();
    let (run_dir, engine_dir) = match (cli.output, &storage) {
        (Some(output), _) => {
            let engine = output.join("engine").join("storage");
            (output, engine)
        }
        (None, Some(dir)) => {
            fs::create_dir_all(dir)?;
            let run = dir.join("runs").join(format!(
                "{}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_secs(),
                new_id(6)
            ));
            (run, dir.join("engine"))
        }
        (None, None) => bail!("--output or [storage].dir required"),
    };
    // The shared runtime reopens its native storage, so its --output may be reused.
    // ReadySet's RocksDB locks refuse a second live process on the same storage.
    ensure!(
        shared || !run_dir.exists(),
        "run directory must not exist: {}",
        run_dir.display()
    );
    let listen = match (cli.listen, cli.port) {
        (Some(addr), None) => addr.parse()?,
        (None, Some(port)) => SocketAddr::from(([127, 0, 0, 1], port)),
        (None, None) => match file.listen.and_then(|l| l.http) {
            Some(addr) => addr.parse()?,
            None => SocketAddr::from(([127, 0, 0, 1], 8001)),
        },
        (Some(_), Some(_)) => bail!("use --listen OR --port"),
    };
    let interval = match cli.interval {
        Some(s) => duration(&format!("{s}s"))?,
        None => duration(replication.poll_interval.as_deref().unwrap_or("1s"))?,
    };
    let max_staleness = match cli.max_staleness {
        Some(s) => duration(&format!("{s}s"))?,
        None => duration(replication.max_staleness.as_deref().unwrap_or("10s"))?,
    };
    let retry_backoff = duration(replication.retry_backoff.as_deref().unwrap_or("5s"))?;
    ensure!(
        retry_backoff >= Duration::from_secs(1) && retry_backoff <= Duration::from_secs(300),
        "replication.retry_backoff must be within 1s..300s"
    );
    let cache_file = file.cache.unwrap_or_default();
    let partial = cli.partial || cache_file.partial.unwrap_or(false);
    let memory_limit_bytes = cli
        .memory_limit_mb
        .or(cache_file.memory_limit_mb)
        .map(|mb| mb.checked_mul(1024 * 1024).context("memory limit overflow"))
        .transpose()?;
    let cache = CacheConfig {
        partial,
        memory_limit_bytes,
        upquery_timeout: Duration::from_millis(
            cli.upquery_timeout_ms
                .or(cache_file.upquery_timeout_ms)
                .unwrap_or(5000),
        ),
    };
    cache.validate(cli.oracle)?;
    // The shared runtime reopens storage keyed by instance_id: a generated id must persist
    // even when only --output is given, or every start would silently begin from scratch.
    let id_dir = match (&storage, shared) {
        (Some(dir), _) => Some(dir.clone()),
        (None, true) => {
            fs::create_dir_all(&run_dir)?;
            Some(run_dir.clone())
        }
        (None, false) => None,
    };
    let instance_id = instance_id(file.instance_id, id_dir.as_deref())?;
    Ok(Settings {
        tables,
        endpoint: endpoint.trim_end_matches('/').to_owned(),
        user,
        password: std::env::var("DATABEND_PASSWORD").unwrap_or_default(),
        declaration,
        run_dir,
        engine_dir,
        instance_id,
        listen,
        interval,
        max_staleness,
        retry_backoff,
        route_journal: cli.route_journal,
        fault_injection: cli.enable_fault_injection,
        oracle: cli.oracle,
        cache,
    })
}

fn url_ok(endpoint: &str) -> Result<()> {
    let rest = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .context("endpoint must be an http(s) origin")?;
    let host = rest.trim_end_matches('/');
    ensure!(
        !host.is_empty()
            && !host.contains('/')
            && !host.contains('@')
            && !host.contains('?')
            && !host.contains('#'),
        "endpoint must be a bare HTTP(S) origin without credentials or path"
    );
    Ok(())
}

#[derive(Default, Clone, serde::Serialize)]
struct Counters {
    readyset: u64,
    databend: u64,
    empty_local: u64,
    sync_batches: u64,
    cache_ddl: u64,
    /// Why each query start was not answered locally (diagnostic; see `count_forward`).
    forward_reasons: std::collections::BTreeMap<&'static str, u64>,
}

/// Routing state. Same meaning as the gateway's fields; held only briefly, never across I/O.
#[derive(Default)]
struct RouteState {
    /// Last completed capture covered every forwarded statement (see `sync_once`).
    ready: bool,
    failed: Option<String>,
    /// An upstream outcome is unknown (possible unobserved write): local reads stay off.
    uncertain: bool,
    /// Upstream queries reported Running/Starting, not yet terminal.
    inflight: HashSet<String>,
    active_forwards: usize,
    /// Bumped whenever a forwarded statement starts or reaches a terminal state.
    forward_epoch: u64,
    last_sync: Option<Instant>,
    sequence: u64,
    rows: u64,
    counters: Counters,
    /// Timing of the last batch with changes, and of the initial snapshot.
    last_batch: Value,
    snapshot: Value,
    /// Why the last completed capture left `ready` false (diagnostic).
    not_ready: Option<&'static str>,
}

impl RouteState {
    /// First reason local reads are off, or `None` when they are allowed.
    fn unavailable(&self, now: Instant, max_staleness: Duration) -> Option<&'static str> {
        if self.failed.is_some() {
            Some("table_failed")
        } else if self.uncertain {
            Some("uncertain_upstream")
        } else if !self.inflight.is_empty() {
            Some("upstream_inflight")
        } else if self.active_forwards != 0 {
            Some("active_forward")
        } else if !self.ready {
            Some(self.not_ready.unwrap_or("not_ready"))
        } else if !self
            .last_sync
            .is_some_and(|t| now.saturating_duration_since(t) <= max_staleness)
        {
            Some("stale")
        } else {
            None
        }
    }
    fn available(&self, now: Instant, max_staleness: Duration) -> bool {
        self.unavailable(now, max_staleness).is_none()
    }
}

struct Stored {
    base: Map<String, Value>,
    rows: Vec<Value>,
    page_size: usize,
    bytes: usize,
}

/// Live local results only. A result leaves at /final, /kill or TTL, so the bound limits
/// concurrently open local queries, never the local read rate.
type LocalResults = HashMap<String, (Instant, Stored)>;

/// Whether serving `page` gives the client the last rows of the result.
fn last_page(stored: &Stored, page: usize) -> bool {
    (page + 1) * stored.page_size >= stored.rows.len()
}

/// Local results whose last page the client already received. databend-jdbc never calls
/// /final after reading a result to the end (Databend keeps such a query until
/// `result_timeout_secs`), so these must not hold one of the `MAX_LOCAL_RESULTS` live slots.
/// They are kept oldest first, for the TTL or until `MAX_DRAINED` entries / `MAX_DRAINED_BYTES`,
/// so a prompt /final, /kill, state poll or last-page retry is still answered locally.
#[derive(Default)]
struct Drained {
    order: std::collections::VecDeque<(Instant, String)>,
    entries: HashMap<String, (Instant, Stored)>,
    bytes: usize,
}

impl Drained {
    /// Returns the IDs evicted to stay within bounds.
    fn insert(&mut self, id: String, expires: Instant, stored: Stored) -> Vec<String> {
        self.bytes += stored.bytes;
        self.order.push_back((expires, id.clone()));
        if let Some((_, old)) = self.entries.insert(id, (expires, stored)) {
            self.bytes -= old.bytes;
        }
        self.prune(Instant::now())
    }

    fn remove(&mut self, id: &str) -> Option<(Instant, Stored)> {
        let entry = self.entries.remove(id)?;
        self.bytes -= entry.1.bytes;
        Some(entry)
    }

    fn contains(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    /// Drop expired entries, then the oldest while over a bound. `order` may hold stale
    /// positions of entries already removed; it is bounded too.
    fn prune(&mut self, now: Instant) -> Vec<String> {
        let mut evicted = Vec::new();
        while let Some((expires, _)) = self.order.front() {
            let over = self.entries.len() > MAX_DRAINED
                || self.bytes > MAX_DRAINED_BYTES
                || self.order.len() > 2 * MAX_DRAINED;
            if !over && *expires > now {
                break;
            }
            let Some((expires, id)) = self.order.pop_front() else {
                break;
            };
            if self.entries.get(&id).is_some_and(|(e, _)| *e == expires)
                && let Some((_, stored)) = self.entries.remove(&id)
            {
                self.bytes -= stored.bytes;
                evicted.push(id);
            }
        }
        evicted
    }
}

/// Bounded FIFO memory of finalized client-chosen local query IDs (see `MAX_FINALIZED`).
#[derive(Default)]
struct Finalized {
    order: std::collections::VecDeque<(Instant, String)>,
    ids: HashSet<String>,
}

impl Finalized {
    fn prune(&mut self, now: Instant) {
        while self
            .order
            .front()
            .is_some_and(|(expires, _)| *expires <= now || self.order.len() > MAX_FINALIZED)
        {
            if let Some((_, id)) = self.order.pop_front() {
                self.ids.remove(&id);
            }
        }
    }
    fn insert(&mut self, id: &str) {
        let now = Instant::now();
        if self.ids.insert(id.to_owned()) {
            self.order
                .push_back((now + LOCAL_RESULT_TTL, id.to_owned()));
        }
        self.prune(now);
    }
    fn contains(&mut self, id: &str) -> bool {
        self.prune(Instant::now());
        self.ids.contains(id)
    }
}

struct App {
    s: Settings,
    auth: String,
    http: reqwest::Client,
    engine: CacheEngine,
    tables: std::collections::BTreeMap<String, Arc<TableState>>,
    names: Names,
    /// Upstream account's default role, learned from the first successful DESCRIBE.
    /// Shared mode learns it in the background (Databend may be down at start).
    default_role: StdMutex<Option<String>>,
    state: StdMutex<RouteState>,
    results: StdMutex<LocalResults>,
    /// Lock order: `results` before `finalized`.
    finalized: StdMutex<Finalized>,
    /// Lock order: `results`, then `drained`, then `finalized`.
    drained: StdMutex<Drained>,
    passive: StdMutex<HashMap<String, Instant>>,
    /// Cache DDL is serialized, like the gateway's single request lock.
    control: tokio::sync::Mutex<()>,
    /// Legacy-only capture fence. Shared `/health` never blocks unrelated tables: each
    /// table reports its own last completed checkpoint and current `syncing` flag.
    capturing: tokio::sync::Mutex<()>,
    routes_journal: Option<Arc<Journal>>,
    /// Operational events (table failures, retries, recoveries, fail-closed), one JSON
    /// object per line, also mirrored to stderr for journald / docker logs.
    events: Arc<Journal>,
    stop: watch::Sender<bool>,
}

struct TableState {
    syncing: std::sync::atomic::AtomicBool,
    names: Names,
    state: StdMutex<RouteState>,
}

impl TableState {
    fn lock(&self) -> std::sync::MutexGuard<'_, RouteState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

enum CacheEngine {
    Legacy(Arc<Engine>),
    Shared(Arc<readyset_databend::shared_engine::SharedEngine>),
}
impl CacheEngine {
    fn legacy(&self) -> Result<&Arc<Engine>> {
        match self {
            Self::Legacy(e) => Ok(e),
            _ => bail!("legacy operation on shared runtime"),
        }
    }
    async fn query(&self, sql: &str, database: &str) -> Result<(ReadOutcome, &'static str)> {
        match self {
            Self::Legacy(e) => e.query(sql, database).await.map(|o| {
                let why = match &o {
                    ReadOutcome::Matched { .. } => "local",
                    ReadOutcome::LimitMiss => "limit_miss",
                    ReadOutcome::NotMatched => "not_matched",
                };
                (o, why)
            }),
            Self::Shared(e) => e.query_explained(sql, database).await,
        }
    }
    fn cache_status(&self) -> Value {
        match self {
            Self::Legacy(e) => e.cache_status(),
            Self::Shared(e) => e.cache_status(),
        }
    }
    fn poison(&self) {
        match self {
            Self::Legacy(e) => e.poison(),
            Self::Shared(e) => e.poison(),
        }
    }
    async fn shutdown(&self) {
        match self {
            Self::Legacy(e) => e.shutdown().await,
            Self::Shared(e) => e.shutdown().await,
        }
    }
}

type Headers = Vec<(HeaderName, HeaderValue)>;

fn reply(status: StatusCode, body: impl Into<Bytes>, headers: Headers) -> Response {
    let mut response = Response::new(Body::from(body.into()));
    *response.status_mut() = status;
    let h = response.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    for (k, v) in headers {
        // Several cookies are several Set-Cookie headers; everything else is single-valued.
        if k == header::SET_COOKIE {
            h.append(k, v);
        } else {
            h.insert(k, v);
        }
    }
    response
}

fn error_body(message: &str) -> Bytes {
    Bytes::from(json!({ "error": message }).to_string())
}

fn route(name: &'static str) -> Headers {
    vec![(
        HeaderName::from_static("x-readyset-route"),
        HeaderValue::from_static(name),
    )]
}

fn cache_control_sql(sql: &str) -> bool {
    // Reserve the cache-control namespace; the native parser decides what is accepted.
    // Comments between the leading keywords are skipped so that malformed cache DDL is
    // rejected locally instead of being forwarded.
    let bytes = sql.as_bytes();
    let mut pos = 0;
    let mut words = Vec::new();
    while words.len() < 3 {
        loop {
            let rest = &sql[pos..];
            if let Some(c) = rest.chars().next()
                && c.is_whitespace()
            {
                pos += c.len_utf8();
            } else if rest.starts_with("--") || rest.starts_with('#') {
                pos += rest.find('\n').map_or(rest.len(), |i| i + 1);
            } else if let Some(comment) = rest.strip_prefix("/*") {
                match comment.find("*/") {
                    Some(i) => pos += i + 4,
                    None => break,
                }
            } else {
                break;
            }
        }
        let start = pos;
        while pos < bytes.len()
            && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_')
            && !(pos == start && bytes[pos].is_ascii_digit())
        {
            pos += 1;
        }
        if pos == start {
            break;
        }
        words.push(sql[start..pos].to_ascii_uppercase());
    }
    let w: Vec<&str> = words.iter().map(String::as_str).collect();
    matches!(
        w.as_slice(),
        ["CREATE", "CACHE", ..]
            | ["DROP", "CACHE", ..]
            | ["SHOW", "CACHES", ..]
            | ["DROP", "ALL", "CACHES"]
    ) || matches!(
        w.as_slice(),
        ["CREATE", "DEEP" | "SHALLOW", "CACHE"] | ["SHOW", "DEEP" | "SHALLOW", "CACHES"]
    )
}

fn safe_headers(headers: &HeaderMap) -> bool {
    headers.keys().all(|k| {
        let k = k.as_str();
        !k.starts_with("x-databend-")
            || matches!(
                k,
                "x-databend-route-hint" | "x-databend-query-id" | "x-databend-sticky-node"
            )
    })
}

fn small_int(v: &Value) -> bool {
    v.as_i64().is_some_and(|n| 0 < n && n <= 10000) && !v.is_f64()
}

/// Never execute a local read under settings, role changes, transactions or unknown state.
fn safe_session(body: &Map<String, Value>, default_role: Option<&str>) -> bool {
    if body
        .keys()
        .any(|k| !matches!(k.as_str(), "sql" | "session" | "pagination"))
    {
        return false;
    }
    let empty = Map::new();
    let s = match body.get("session") {
        None => &empty,
        Some(Value::Object(s)) => s,
        Some(_) => return false,
    };
    const ALLOWED: [&str; 9] = [
        "database",
        "catalog",
        "settings",
        "txn_state",
        "need_sticky",
        "need_keep_alive",
        "role",
        "internal",
        // databend-jdbc's Connection.setAutoCommit flag. Databend ignores it (a transaction
        // starts only with an explicit BEGIN, reported in txn_state), so it does not change
        // what a read sees.
        "autoCommit",
    ];
    if s.keys().any(|k| !ALLOWED.contains(&k.as_str())) {
        return false;
    }
    if s.get("autoCommit").is_some_and(|v| !v.is_boolean()) {
        return false;
    }
    if let Some(role) = s.get("role")
        && (default_role.is_none() || role.as_str() != default_role)
    {
        return false;
    }
    if s.get("settings").is_some_and(|v| !v.is_object()) {
        return false;
    }
    if ["need_sticky", "need_keep_alive"]
        .iter()
        .any(|k| s.get(*k).is_some_and(|v| !v.is_boolean()))
    {
        return false;
    }
    if let Some(internal) = s.get("internal") {
        let Some(text) = internal.as_str() else {
            return false;
        };
        let Ok(Value::Object(i)) = serde_json::from_str::<Value>(text) else {
            return false;
        };
        let keys: HashSet<&str> = i.keys().map(String::as_str).collect();
        if keys != HashSet::from(["last_node_id", "last_query_ids"])
            || !i["last_node_id"].is_string()
            || !i["last_query_ids"]
                .as_array()
                .is_some_and(|ids| ids.iter().all(Value::is_string))
        {
            return false;
        }
    }
    let pagination = match body.get("pagination") {
        None => &empty,
        Some(Value::Object(p)) => p,
        Some(_) => return false,
    };
    s.get("catalog").is_none_or(|c| c == "default")
        && s.get("settings")
            .is_none_or(|v| v.is_null() || v.as_object().is_some_and(Map::is_empty))
        // databend-jdbc sends `"txn_state": null` before its first reply; Databend reads null
        // as absent (autocommit).
        && s.get("txn_state")
            .is_none_or(|t| t.is_null() || t == "AutoCommit")
        && s.get("need_sticky") != Some(&Value::Bool(true))
        && s.get("need_keep_alive") != Some(&Value::Bool(true))
        && s.get("database").is_none_or(Value::is_string)
        && pagination.iter().all(|(k, v)| match k.as_str() {
            "wait_time_secs" => small_int(v),
            // Client buffer/page-size hints (databend-jdbc: 5,000,000 / 10,000). A local
            // result is bounded by its own limits and pages at most MAX_LOCAL_PAGE_ROWS rows.
            "max_rows_per_page" | "max_rows_in_buffer" => {
                v.as_u64().is_some_and(|n| n > 0) && !v.is_f64()
            }
            _ => false,
        })
}

fn session_database(body: &Map<String, Value>) -> String {
    body.get("session")
        .and_then(|s| s.get("database"))
        .and_then(Value::as_str)
        .unwrap_or("default")
        .to_owned()
}

enum Lifecycle {
    State,
    Page(String),
    Final,
    Kill,
}

fn lifecycle(path: &str) -> Option<(String, Lifecycle)> {
    let rest = path.strip_prefix("/v1/query/")?;
    let (id, tail) = match rest.split_once('/') {
        Some((id, tail)) => (id, Some(tail)),
        None => (rest, None),
    };
    if id.is_empty()
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return None;
    }
    let kind = match tail {
        None => Lifecycle::State,
        Some("final") => Lifecycle::Final,
        Some("kill") => Lifecycle::Kill,
        Some(t) => {
            let n = t.strip_prefix("page/")?;
            if n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            Lifecycle::Page(n.to_owned())
        }
    };
    Some((id.to_owned(), kind))
}

fn local_page(stored: &Stored, prefix: &str, page: usize) -> Value {
    let mut data = stored.base.clone();
    let start = page * stored.page_size;
    let end = (start + stored.page_size).min(stored.rows.len());
    data.insert(
        "data".into(),
        Value::Array(stored.rows[start.min(end)..end].to_vec()),
    );
    data.insert(
        "next_uri".into(),
        Value::String(if start + stored.page_size < stored.rows.len() {
            format!("{prefix}/page/{}", page + 1)
        } else {
            format!("{prefix}/final")
        }),
    );
    Value::Object(data)
}

impl App {
    fn lock(&self) -> std::sync::MutexGuard<'_, RouteState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn results(&self) -> std::sync::MutexGuard<'_, LocalResults> {
        self.results.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn drained(&self) -> std::sync::MutexGuard<'_, Drained> {
        self.drained.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// An evicted client-chosen ID is remembered like a finalized one, so a late lifecycle
    /// call never reaches Databend and the ID is not reused locally.
    fn remember_evicted(&self, evicted: Vec<String>) {
        let ids: Vec<_> = evicted
            .into_iter()
            .filter(|id| !id.starts_with("rs_local_"))
            .collect();
        if !ids.is_empty() {
            let mut finalized = self.finalized.lock().unwrap_or_else(|e| e.into_inner());
            for id in &ids {
                finalized.insert(id);
            }
        }
    }

    /// Keep a result whose last page the client has received (caller holds `results`).
    fn park_drained(&self, id: String, expires: Instant, stored: Stored) {
        let evicted = self.drained().insert(id, expires, stored);
        self.remember_evicted(evicted);
    }

    fn table_available(&self, id: &str) -> bool {
        self.table_unavailable(id).is_none()
    }

    /// A cache may be declared on a table that finished its initial load and is not failed.
    /// Freshness is not required: the new cache is fenced to the local base table, and every
    /// read still checks `table_available`, so a lagging table just keeps reading upstream.
    /// (Requiring freshness rejected back-to-back CREATE CACHE: building one cache pauses
    /// that table's sync for the native index build.)
    fn table_loaded(&self, id: &str) -> bool {
        if matches!(&self.engine, CacheEngine::Shared(e) if e.is_failed()) {
            return false;
        }
        self.tables.get(id).is_some_and(|t| {
            let state = t.lock();
            state.failed.is_none() && state.last_sync.is_some()
        })
    }

    /// First reason this table's local reads are off, or `None` when they are allowed.
    fn table_unavailable(&self, id: &str) -> Option<&'static str> {
        if let CacheEngine::Shared(e) = &self.engine {
            if e.is_failed() {
                return Some("engine_failed");
            }
            if e.source_busy(id) {
                return Some("source_applying");
            }
        }
        let global = self.lock();
        let Some(table) = self.tables.get(id) else {
            return Some("unknown_table");
        };
        let state = table.lock();
        if global.failed.is_some() {
            Some("engine_failed")
        } else if global.uncertain {
            Some("uncertain_upstream")
        } else if global.active_forwards != 0 {
            Some("active_forward")
        } else if !global.inflight.is_empty() {
            Some("upstream_inflight")
        } else if state.forward_epoch != global.forward_epoch {
            Some("forward_epoch")
        } else {
            state.unavailable(Instant::now(), self.s.max_staleness)
        }
    }

    fn ready(&self) -> bool {
        if self.tables.is_empty() {
            self.lock().available(Instant::now(), self.s.max_staleness)
        } else {
            self.tables.keys().any(|id| self.table_available(id))
        }
    }

    /// The cached table a read targets (shared mode) and why it cannot be local, if so.
    fn read_route(&self, sql: &str, database: &str) -> (Option<String>, Option<&'static str>) {
        match &self.engine {
            CacheEngine::Legacy(_) => (
                None,
                self.lock()
                    .unavailable(Instant::now(), self.s.max_staleness),
            ),
            CacheEngine::Shared(engine) => match engine.read_source(sql, database) {
                Some(id) => {
                    let why = self.table_unavailable(&id);
                    (Some(id), why)
                }
                None => (None, Some("not_cached_table")),
            },
        }
    }

    fn count_forward(&self, table: Option<&str>, reason: &'static str) {
        *self
            .lock()
            .counters
            .forward_reasons
            .entry(reason)
            .or_default() += 1;
        if let Some(state) = table.and_then(|t| self.tables.get(t)) {
            *state
                .lock()
                .counters
                .forward_reasons
                .entry(reason)
                .or_default() += 1;
        }
    }

    /// One operational event: appended to `events.jsonl` and written to stderr.
    fn event(&self, event: &str, table: Option<&str>, detail: Value) {
        let utc = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64());
        let mut line = Map::new();
        line.insert("utc".into(), json!(utc));
        line.insert("event".into(), json!(event));
        line.insert("instance_id".into(), json!(self.s.instance_id));
        if let Some(table) = table {
            line.insert("table".into(), json!(table));
        }
        if let Value::Object(detail) = detail {
            line.extend(detail);
        }
        let line = Value::Object(line);
        eprintln!("{line}");
        self.events.write(&line);
    }

    fn fail_source(&self, id: &str, reason: String) {
        if let Some(table) = self.tables.get(id) {
            let mut state = table.lock();
            state.ready = false;
            // Latest reason: a table may fail, recover itself and fail again.
            state.failed = Some(reason);
        }
        if let CacheEngine::Shared(engine) = &self.engine {
            engine.fail_source(id);
        }
    }

    fn status(&self) -> Value {
        if !self.tables.is_empty() {
            let tables: Vec<_> = self.tables.iter().map(|(id,table)| {
                let unavailable = self.table_unavailable(id);
                let ready = unavailable.is_none();
                let state = table.lock();
                json!({"table":id,"stream":table.names.stream,"checkpoint":state.sequence,"rows":state.rows,
                    "ready":ready,"unavailable":unavailable,"syncing":table.syncing.load(std::sync::atomic::Ordering::Acquire),
                    "applying":matches!(&self.engine,CacheEngine::Shared(e) if e.source_busy(id)),
                    "last_sync_age_ms":state.last_sync.map(|t| t.elapsed().as_millis()),"failed":state.failed,"phase":if state.failed.is_some() {"error"} else if state.last_sync.is_some() {"online"} else {"snapshotting"},
                    "snapshot":state.snapshot,"last_batch":state.last_batch,"counters":state.counters})
            }).collect();
            let global = self.lock();
            let failed = global.failed.clone().or_else(|| {
                matches!(&self.engine, CacheEngine::Shared(e) if e.is_failed()).then(|| {
                    "shared native runtime failed closed (possibly cancelled migration)".to_owned()
                })
            });
            return json!({"generation":self.names.generation,"instance_id":self.s.instance_id,
                "ready":tables.iter().any(|t| t["ready"]==true),"all_tables_ready":tables.iter().all(|t| t["ready"]==true),
                "failed":failed,"uncertain_upstream":global.uncertain,"inflight":global.inflight.len(),
                "checkpoint":if tables.len()==1 {tables[0]["checkpoint"].clone()} else {Value::Null},
                "rows":tables.iter().filter_map(|t| t["rows"].as_u64()).sum::<u64>(),"tables":tables,
                "counters":global.counters,"cache":self.engine.cache_status(),"mode":"production","engine_mode":"shared",
                "engine_pid":std::process::id(),"single_process":true,"fault_injection":self.s.fault_injection});
        }
        let st = self.lock();
        json!({
            "generation": self.names.generation,
            "instance_id": self.s.instance_id,
            "checkpoint": st.sequence,
            "ready": st.available(Instant::now(), self.s.max_staleness),
            "failed": st.failed,
            "uncertain_upstream": st.uncertain,
            "inflight": st.inflight.len(),
            "rows": st.rows,
            "counters": st.counters,
            "mode": if self.s.oracle { "oracle" } else { "production" },
            "snapshot": st.snapshot,
            "last_batch": st.last_batch,
            "cache": self.engine.cache_status(),
            "engine_pid": std::process::id(),
            "single_process": true,
            "fault_injection": self.s.fault_injection,
        })
    }

    fn write_status(&self, name: &str) {
        let _ = fs::write(self.s.run_dir.join(name), format!("{:#}\n", self.status()));
    }

    /// Fail closed: no further engine operation, no further capture; reads go upstream.
    fn fail(&self, reason: String) {
        self.engine.poison();
        {
            let mut st = self.lock();
            st.ready = false;
            st.failed.get_or_insert(reason.clone());
        }
        self.event("engine_failed_closed", None, json!({"reason": reason}));
        let _ = self.stop.send(true);
    }

    fn local_bytes(results: &LocalResults) -> usize {
        // Budget retained JSON payload, not a claim about exact heap usage.
        results.values().map(|(_, s)| s.bytes).sum()
    }

    /// Whether a lifecycle request for this ID belongs to a local result (live, generated,
    /// or a finalized client-chosen ID). Caller holds `results`.
    fn local_id(&self, results: &LocalResults, id: &str) -> bool {
        results.contains_key(id)
            || id.starts_with("rs_local_")
            || self.drained().contains(id)
            || self
                .finalized
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(id)
    }

    fn local_query_id(
        &self,
        headers: &HeaderMap,
        results: &LocalResults,
    ) -> Result<String, String> {
        let qid = headers
            .get("x-databend-query-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("rs_local_{}", uuid::Uuid::new_v4().simple()));
        if qid.is_empty()
            || qid.len() > 128
            || !qid
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err("invalid query ID".into());
        }
        if results.contains_key(&qid)
            || self.drained().contains(&qid)
            || self
                .finalized
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(&qid)
        {
            return Err("query ID already used locally".into());
        }
        Ok(qid)
    }

    /// Store a complete local result and return its first page, or `None` when it exceeds
    /// the local budget (never a partial local success).
    fn local_response(
        &self,
        result: &Value,
        body: &Map<String, Value>,
        qid: String,
        template: Option<&str>,
        results: &mut LocalResults,
    ) -> Option<Response> {
        let prefix = format!("/v1/query/{qid}");
        let error = if result["ok"] == true {
            Value::Null
        } else {
            json!({"code": 1005, "message": result["error"].as_str().unwrap_or("cache control failed")})
        };
        let rows = result["data"].as_array().cloned().unwrap_or_default();
        let n = rows.len();
        let mut base = Map::new();
        base.insert("id".into(), json!(qid));
        base.insert("session_id".into(), json!(""));
        base.insert("node_id".into(), json!(""));
        base.insert(
            "state".into(),
            json!(if error.is_null() {
                "Succeeded"
            } else {
                "Failed"
            }),
        );
        base.insert(
            "session".into(),
            body.get("session")
                .cloned()
                .unwrap_or_else(|| json!({"database": "default"})),
        );
        base.insert("error".into(), error);
        base.insert("warnings".into(), json!([]));
        base.insert("has_result_set".into(), json!(true));
        base.insert(
            "schema".into(),
            result.get("schema").cloned().unwrap_or_else(|| json!([])),
        );
        base.insert("affect".into(), Value::Null);
        base.insert("result_timeout_secs".into(), json!(60));
        base.insert(
            "stats".into(),
            json!({"running_time_ms": 0, "scan_progress": {"rows": n, "bytes": 0},
                   "write_progress": {"rows": 0, "bytes": 0}, "result_progress": {"rows": n, "bytes": 0}}),
        );
        base.insert("stats_uri".into(), json!(prefix));
        base.insert("final_uri".into(), json!(format!("{prefix}/final")));
        base.insert("kill_uri".into(), json!(format!("{prefix}/kill")));
        let size = serde_json::to_vec(&base).map_or(usize::MAX, |b| b.len())
            + serde_json::to_vec(&rows).map_or(usize::MAX, |b| b.len());
        if size > MAX_LOCAL_QUERY_BYTES || Self::local_bytes(results) + size > MAX_LOCAL_TOTAL_BYTES
        {
            return None;
        }
        let pagination = body.get("pagination");
        let limit = |k: &str| {
            pagination
                .and_then(|p| p.get(k))
                .and_then(Value::as_u64)
                .unwrap_or(1000) as usize
        };
        let stored = Stored {
            base,
            rows,
            page_size: limit("max_rows_per_page")
                .min(limit("max_rows_in_buffer"))
                .clamp(1, MAX_LOCAL_PAGE_ROWS),
            bytes: size,
        };
        let page = local_page(&stored, &prefix, 0);
        let expires = Instant::now() + LOCAL_RESULT_TTL;
        if last_page(&stored, 0) {
            self.park_drained(qid, expires, stored);
        } else {
            results.insert(qid, (expires, stored));
        }
        let mut headers = route("readyset");
        headers.push((
            HeaderName::from_static("x-readyset-template"),
            HeaderValue::from_str(template.unwrap_or("cache-control"))
                .unwrap_or(HeaderValue::from_static("cache-control")),
        ));
        Some(reply(StatusCode::OK, page.to_string(), headers))
    }

    async fn handle(
        &self,
        method: Method,
        uri: axum::http::Uri,
        headers: HeaderMap,
        raw: Bytes,
    ) -> Response {
        let path = uri.path();
        if path == "/health" && method == Method::GET {
            // Never report a checkpoint while a capture that may predate the caller's last
            // write is still being applied.
            let _capturing = self.capturing.lock().await;
            return reply(StatusCode::OK, self.status().to_string(), vec![]);
        }
        if path == "/__fault/table" && method == Method::POST && self.s.fault_injection {
            let body: Value = serde_json::from_slice(&raw).unwrap_or(Value::Null);
            let id = body["table"].as_str().unwrap_or_default();
            if !self.tables.contains_key(id) {
                return reply(
                    StatusCode::BAD_REQUEST,
                    error_body("unknown source table"),
                    vec![],
                );
            }
            self.fail_source(id, "injected table failure".into());
            return reply(StatusCode::OK, r#"{"ok":true}"#, vec![]);
        }
        if path == "/__fault/engine" && method == Method::POST && self.s.fault_injection {
            self.fail("injected engine failure".into());
            return reply(StatusCode::OK, r#"{"ok":true}"#, vec![]);
        }
        if path == "/__fault/native" && method == Method::GET && self.s.fault_injection {
            let CacheEngine::Shared(engine) = &self.engine else {
                return reply(
                    StatusCode::BAD_REQUEST,
                    error_body("shared runtime required"),
                    vec![],
                );
            };
            return match engine.native_fault_targets().await {
                Ok(targets) => reply(StatusCode::OK, targets.to_string(), vec![]),
                Err(e) => reply(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_body(&format!("{e:#}")),
                    vec![],
                ),
            };
        }
        let start = path == "/v1/query" && method == Method::POST;
        if method == Method::POST && SESSION_PATHS.contains(&path) {
            return self.session_endpoint(&uri, &headers, raw).await;
        }
        let cycle = lifecycle(path);
        let lifecycle_ok = cycle.as_ref().is_some_and(|(_, kind)| {
            method == Method::GET || (method == Method::POST && matches!(kind, Lifecycle::Kill))
        });
        if !start && !lifecycle_ok {
            return reply(
                StatusCode::NOT_FOUND,
                error_body("unsupported endpoint"),
                vec![],
            );
        }
        let body: Map<String, Value> = if start {
            match serde_json::from_slice::<Value>(&raw) {
                Ok(Value::Object(body))
                    if body
                        .get("sql")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.trim().is_empty()) =>
                {
                    body
                }
                Ok(_) => {
                    return reply(
                        StatusCode::BAD_REQUEST,
                        error_body("sql must be a nonempty string"),
                        vec![],
                    );
                }
                Err(error) => {
                    return reply(
                        StatusCode::BAD_REQUEST,
                        error_body(&error.to_string()),
                        vec![],
                    );
                }
            }
        } else {
            Map::new()
        };
        let now = Instant::now();
        {
            let mut results = self.results();
            results.retain(|_, (expires, _)| *expires > now);
            let evicted = self.drained().prune(now);
            self.remember_evicted(evicted);
            if let Some((id, kind)) = &cycle
                && self.local_id(&results, id)
            {
                return self.local_lifecycle(&mut results, id, kind);
            }
        }
        let sql = body.get("sql").and_then(Value::as_str).unwrap_or_default();
        let database = session_database(&body);
        let default_role = self
            .default_role
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let sane = safe_session(&body, default_role.as_deref())
            && safe_headers(&headers)
            && (!self.tables.is_empty()
                || database == "default"
                || database == self.s.declaration.database);
        if start && cache_control_sql(sql) {
            return self
                .cache_control(&body, &headers, sql, &database, sane)
                .await;
        }
        let (read_table, unavailable) = if start && sane {
            self.read_route(sql, &database)
        } else {
            (None, None)
        };
        let mut forward_reason = if !start {
            None
        } else if !sane {
            Some("unsafe_session")
        } else if unavailable.is_some() {
            unavailable
        } else if self.results().len() >= MAX_LOCAL_RESULTS {
            Some("local_results_full")
        } else {
            None
        };
        if start && forward_reason.is_none() {
            match self.engine.query(sql, &database).await {
                Ok((
                    ReadOutcome::Matched {
                        template,
                        schema,
                        data,
                    },
                    _,
                )) => {
                    let mut results = self.results();
                    let qid = match self.local_query_id(&headers, &results) {
                        Ok(qid) => qid,
                        Err(e) => {
                            let status = if e == "query ID already used locally" {
                                StatusCode::CONFLICT
                            } else {
                                StatusCode::BAD_REQUEST
                            };
                            return reply(status, error_body(&e), vec![]);
                        }
                    };
                    let empty = data.is_empty();
                    let result = json!({"ok": true, "schema": schema, "data": data});
                    if let Some(response) =
                        self.local_response(&result, &body, qid, Some(&template), &mut results)
                    {
                        drop(results);
                        let mut st = self.lock();
                        st.counters.readyset += 1;
                        st.counters.empty_local += u64::from(empty);
                        return response;
                    }
                    // Over the local budget: serve this read from upstream, once.
                    forward_reason = Some("local_result_budget");
                }
                Ok((_, why)) => forward_reason = Some(why),
                // Do not send further engine RPCs after an uncertain engine result.
                Err(error) => {
                    forward_reason = Some("engine_error");
                    if let CacheEngine::Shared(engine) = &self.engine {
                        if let Some(id) = engine.read_source(sql, &database) {
                            self.fail_source(&id, format!("engine query failure: {error:#}"));
                        } else {
                            self.fail(format!("engine query failure: {error:#}"));
                        }
                    } else {
                        self.fail(format!("engine query failure: {error:#}"));
                    }
                }
            }
        }
        if let Some(reason) = forward_reason {
            self.count_forward(read_table.as_deref(), reason);
        }
        let passive_start = start
            && sane
            && matches!(&self.engine, CacheEngine::Shared(engine) if engine.read_source(sql,&database).is_some());
        let passive_cycle = if let Some((id, _)) = &cycle {
            let mut passive = self.passive.lock().unwrap_or_else(|e| e.into_inner());
            passive.retain(|_, t| *t > Instant::now());
            passive.contains_key(id)
        } else {
            false
        };
        if passive_start || passive_cycle {
            let outcome = self.forward(&method, &uri, raw, &headers).await;
            return match outcome {
                Ok((status, content, mut kept)) => {
                    if passive_start {
                        self.lock().counters.databend += 1;
                        if let Ok(response) = serde_json::from_slice::<Value>(&content)
                            && let Some(id) = response["id"].as_str()
                        {
                            let mut passive =
                                self.passive.lock().unwrap_or_else(|e| e.into_inner());
                            passive.retain(|_, t| *t > Instant::now());
                            if passive.len() < MAX_INFLIGHT {
                                passive
                                    .insert(id.into(), Instant::now() + Duration::from_secs(3600));
                            }
                        }
                    }
                    if let Some((id, Lifecycle::Final | Lifecycle::Kill)) = &cycle {
                        self.passive
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(id);
                    }
                    kept.extend(route("databend"));
                    reply(status, content, kept)
                }
                Err(_) => reply(
                    StatusCode::BAD_GATEWAY,
                    error_body("upstream read failed; not retried"),
                    vec![],
                ),
            };
        }
        self.forward_and_track(method, &uri, raw, &headers, start, cycle)
            .await
    }

    fn local_lifecycle(&self, results: &mut LocalResults, id: &str, kind: &Lifecycle) -> Response {
        let not_found = |message: &str| {
            reply(
                StatusCode::NOT_FOUND,
                error_body(message),
                route("readyset"),
            )
        };
        let (live, (expires, stored)) = match results.remove(id) {
            Some(entry) => (true, entry),
            None => match self.drained().remove(id) {
                Some(entry) => (false, entry),
                None => return not_found("local query expired or finalized"),
            },
        };
        let page = match kind {
            Lifecycle::Page(number) => match number.parse::<usize>() {
                Ok(page)
                    if number.len() <= 10
                        && number == &page.to_string()
                        && page >= 1
                        && page * stored.page_size < stored.rows.len() =>
                {
                    Some(page)
                }
                _ => None,
            },
            _ => Some(0),
        };
        let Some(page) = page else {
            if live {
                results.insert(id.to_owned(), (expires, stored));
            } else {
                self.park_drained(id.to_owned(), expires, stored);
            }
            return not_found("local page does not exist");
        };
        let prefix = format!("/v1/query/{id}");
        let mut data = local_page(&stored, &prefix, page);
        if matches!(kind, Lifecycle::Final | Lifecycle::Kill) {
            // Release the result now; only a client-chosen ID is remembered (bounded), so a
            // finalized local ID is never forwarded upstream nor reused locally.
            if !id.starts_with("rs_local_") {
                self.finalized
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(id);
            }
            data["data"] = json!([]);
            data["next_uri"] = Value::Null;
        } else if !live || (matches!(kind, Lifecycle::Page(_)) && last_page(&stored, page)) {
            self.park_drained(id.to_owned(), expires, stored);
        } else {
            results.insert(id.to_owned(), (expires, stored));
        }
        reply(StatusCode::OK, data.to_string(), route("readyset"))
    }

    async fn cache_control(
        &self,
        body: &Map<String, Value>,
        headers: &HeaderMap,
        sql: &str,
        database: &str,
        sane: bool,
    ) -> Response {
        // DDL never falls back to Databend, even when the replica/engine is unavailable.
        if !sane {
            return reply(
                StatusCode::BAD_REQUEST,
                error_body(
                    "cache control requires a safe autocommit session and configured source",
                ),
                route("readyset"),
            );
        }
        let _serial = self.control.lock().await;
        let unavailable = self.tables.is_empty() && !self.ready() || {
            let results = self.results();
            results.len() >= MAX_LOCAL_RESULTS
                || Self::local_bytes(&results) > MAX_LOCAL_TOTAL_BYTES - CONTROL_RESPONSE_RESERVE
        };
        if unavailable {
            return reply(
                StatusCode::SERVICE_UNAVAILABLE,
                error_body("cache control requires a ready replica; statement was not forwarded"),
                route("readyset"),
            );
        }
        // Validate the query ID BEFORE any mutation.
        let qid = match self.local_query_id(headers, &self.results()) {
            Ok(qid) => qid,
            Err(e) => return reply(StatusCode::BAD_REQUEST, error_body(&e), vec![]),
        };
        let ddl_started = Instant::now();
        let applied = match &self.engine {
            CacheEngine::Legacy(engine) => engine.cache_ddl(sql, database).await,
            CacheEngine::Shared(engine) => {
                engine
                    .cache_ddl(sql, database, |id| self.table_loaded(id))
                    .await
            }
        };
        // A native cache build pauses its own table's sync for the index build; record how
        // long, so operators can see (and schedule) it. SHOW CACHES changes nothing.
        if !sql.trim_start().to_ascii_uppercase().starts_with("SHOW") {
            self.event(
                "cache_ddl",
                None,
                json!({
                    "sql": sql.chars().take(2000).collect::<String>(),
                    "secs": ddl_started.elapsed().as_secs_f64(),
                    "ok": applied.as_ref().is_ok_and(|r| r["ok"] == true),
                    "rejected": applied.as_ref().ok().and_then(|r| r["rejected"].as_bool()).unwrap_or(false),
                    "error": match &applied {
                        Ok(r) => r.get("error").cloned().unwrap_or(Value::Null),
                        Err(e) => json!(format!("{e:#}")),
                    },
                }),
            );
        }
        let failure = match applied {
            Ok(result) if result["ok"] == true || result["rejected"] == true => {
                if result["ok"] == true {
                    // Audit only. This file is not a crash-recovery catalog.
                    let _ = fs::write(
                        self.s.run_dir.join("cache_catalog.json"),
                        format!("{result:#}\n"),
                    );
                }
                let mut results = self.results();
                match self.local_response(&result, body, qid, None, &mut results) {
                    Some(response) => {
                        drop(results);
                        self.lock().counters.cache_ddl += u64::from(result["ok"] == true);
                        return response;
                    }
                    None => "cache control response exceeded reserved result budget".to_owned(),
                }
            }
            Ok(result) => format!("uncertain cache mutation: {result}"),
            Err(error) => format!("{error:#}"),
        };
        self.fail(format!("engine cache control failure: {failure}"));
        reply(
            StatusCode::BAD_GATEWAY,
            error_body("cache control failed; no retry or upstream forwarding"),
            route("readyset"),
        )
    }

    /// Driver session endpoints: databend-jdbc logs in on connect (and needs the server
    /// version from the reply), heartbeats while results are open, and logs out on close.
    /// They run no SQL, so they are forwarded without touching forward tracking. Login never
    /// issues Databend session tokens through the gateway (clients authenticate to the
    /// gateway, never to Databend directly) and does not advertise Arrow results, which local
    /// reads do not serve; the driver then stays on JSON, its default.
    async fn session_endpoint(
        &self,
        uri: &axum::http::Uri,
        headers: &HeaderMap,
        raw: Bytes,
    ) -> Response {
        let login = uri.path() == "/v1/session/login";
        let target = if login {
            "/v1/session/login?disable_session_token=true"
        } else {
            uri.path()
        };
        let target: axum::http::Uri = match target.parse() {
            Ok(t) => t,
            Err(_) => {
                return reply(StatusCode::BAD_REQUEST, error_body("invalid path"), vec![]);
            }
        };
        let (status, content, mut kept) =
            match self.forward(&Method::POST, &target, raw, headers).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    return reply(
                        StatusCode::BAD_GATEWAY,
                        error_body("gateway/upstream failure"),
                        vec![],
                    );
                }
            };
        let content = if login && status.is_success() {
            match serde_json::from_slice::<Value>(&content) {
                Ok(Value::Object(mut r)) => {
                    for k in [
                        "tokens",
                        "server_max_arrow_result_version",
                        "server_arrow_features",
                    ] {
                        r.remove(k);
                    }
                    Bytes::from(Value::Object(r).to_string())
                }
                _ => {
                    return reply(
                        StatusCode::BAD_GATEWAY,
                        error_body("unexpected upstream login response"),
                        vec![],
                    );
                }
            }
        } else {
            content
        };
        kept.extend(route("databend"));
        reply(status, content, kept)
    }

    async fn forward(
        &self,
        method: &Method,
        uri: &axum::http::Uri,
        raw: Bytes,
        headers: &HeaderMap,
    ) -> Result<(StatusCode, Bytes, Headers)> {
        let target = uri.path_and_query().map_or(uri.path(), |p| p.as_str());
        let mut request = self
            .http
            .request(method.clone(), format!("{}{target}", self.s.endpoint))
            .header(header::AUTHORIZATION, &self.auth)
            .header(header::CONTENT_TYPE, "application/json")
            .timeout(Duration::from_secs(15));
        for (name, value) in headers {
            // Databend cookie sessions (databend-jdbc sends `cookie_enabled`) keep server-side
            // session state such as temporary tables; pass them through both ways.
            if name.as_str().starts_with("x-databend-") || *name == header::COOKIE {
                request = request.header(name, value);
            }
        }
        if method == Method::POST {
            request = request.body(raw);
        }
        let response = request.send().await?;
        let status = response.status();
        let kept: Headers = response
            .headers()
            .iter()
            .filter(|(k, _)| {
                k.as_str().starts_with("x-databend-")
                    || *k == header::CONTENT_TYPE
                    || *k == header::SET_COOKIE
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let body = bounded_body(response, MAX_RESPONSE).await?;
        Ok((status, Bytes::from(body), kept))
    }

    /// Serialize nothing, resubmit nothing: track whether a forwarded statement may still be
    /// changing the source, which keeps local reads off until a later capture covers it.
    async fn forward_and_track(
        &self,
        method: Method,
        uri: &axum::http::Uri,
        raw: Bytes,
        headers: &HeaderMap,
        start: bool,
        cycle: Option<(String, Lifecycle)>,
    ) -> Response {
        if start {
            let mut st = self.lock();
            st.ready = false;
            st.active_forwards += 1;
            st.forward_epoch += 1;
        }
        let outcome = self.forward(&method, uri, raw, headers).await;
        let mut st = self.lock();
        if start {
            st.active_forwards -= 1;
        }
        let failure = match outcome {
            Ok((status, content, mut kept)) => {
                if start {
                    st.counters.databend += 1;
                }
                if status.as_u16() < 300 {
                    match if content.is_empty() {
                        Ok(json!({}))
                    } else {
                        serde_json::from_slice::<Value>(&content)
                    } {
                        Ok(r) => {
                            let qid = r["id"].as_str().filter(|q| !q.is_empty());
                            let state = r["state"].as_str().unwrap_or_default();
                            if start
                                && (qid.is_none()
                                    || !matches!(
                                        state,
                                        "Running" | "Starting" | "Succeeded" | "Failed"
                                    ))
                            {
                                st.uncertain = true;
                            }
                            if let Some(qid) = qid {
                                if matches!(state, "Running" | "Starting") {
                                    if st.inflight.len() >= MAX_INFLIGHT {
                                        st.uncertain = true;
                                    } else {
                                        st.inflight.insert(qid.to_owned());
                                    }
                                } else if matches!(state, "Succeeded" | "Failed") {
                                    st.inflight.remove(qid);
                                    // Needs a post-completion capture, even if it was a write.
                                    st.ready = false;
                                    st.forward_epoch += 1;
                                }
                            }
                            // A final response without a terminal state cannot prove the
                            // statement finished.
                            if let Some((id, Lifecycle::Final)) = &cycle
                                && st.inflight.contains(id)
                            {
                                st.uncertain = true;
                            }
                        }
                        Err(error) => {
                            st.uncertain = true;
                            st.ready = false;
                            st.forward_epoch += 1;
                            return reply(
                                StatusCode::BAD_REQUEST,
                                error_body(&error.to_string()),
                                vec![],
                            );
                        }
                    }
                } else if start {
                    // Conservative: do not infer non-execution from an HTTP status.
                    st.uncertain = true;
                }
                kept.extend(route("databend"));
                return reply(status, content, kept);
            }
            Err(error) => error,
        };
        if start || !st.inflight.is_empty() {
            st.uncertain = true;
        }
        st.ready = false;
        st.forward_epoch += 1;
        let too_large = failure.to_string().contains("too large");
        reply(
            if too_large {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::BAD_GATEWAY
            },
            error_body(if too_large {
                "upstream response too large; SQL not retried"
            } else {
                "gateway/upstream failure; SQL was not retried"
            }),
            vec![],
        )
    }
}

async fn dispatch(State(app): State<Arc<App>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    // Unauthenticated probes for Docker/k8s. They expose only liveness / readiness.
    if parts.method == Method::GET && parts.uri.path() == "/livez" {
        return reply(StatusCode::OK, r#"{"ok":true}"#, vec![]);
    }
    if parts.method == Method::GET && parts.uri.path() == "/readyz" {
        let ready = app.ready();
        return reply(
            if ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            json!({ "ready": ready }).to_string(),
            vec![],
        );
    }
    let headers = parts.headers;
    let auth = headers
        .get(header::AUTHORIZATION)
        .map(HeaderValue::as_bytes)
        .unwrap_or_default();
    // Constant-time comparison of the single configured principal.
    let expected = app.auth.as_bytes();
    let matches = auth.len() == expected.len()
        && auth
            .iter()
            .zip(expected)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0;
    if !matches {
        return reply(
            StatusCode::UNAUTHORIZED,
            error_body("authentication required"),
            vec![(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"readyset-databend\""),
            )],
        );
    }
    if headers.contains_key(header::TRANSFER_ENCODING)
        || headers.get_all(header::CONTENT_LENGTH).iter().count() > 1
    {
        return reply(
            StatusCode::BAD_REQUEST,
            error_body("chunked or ambiguous request bodies unsupported"),
            vec![],
        );
    }
    let raw = match axum::body::to_bytes(body, MAX_BODY).await {
        Ok(raw) => raw,
        Err(_) => {
            return reply(
                StatusCode::PAYLOAD_TOO_LARGE,
                error_body("request too large"),
                vec![],
            );
        }
    };
    let method = parts.method;
    let uri = parts.uri;
    let journal = app.routes_journal.as_ref().map(|_| {
        let keys = |v: Option<&Value>| -> Vec<String> {
            v.and_then(Value::as_object)
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default()
        };
        let payload: Option<Value> = (method == Method::POST && uri.path() == "/v1/query")
            .then(|| serde_json::from_slice(&raw).ok())
            .flatten();
        json!({
            "method": method.as_str(),
            "path": uri.path_and_query().map_or(uri.path(), |p| p.as_str()),
            "databend_headers": headers.keys().filter(|k| k.as_str().starts_with("x-databend-")).map(|k| k.as_str()).collect::<Vec<_>>(),
            "body_keys": keys(payload.as_ref()),
            "session_keys": keys(payload.as_ref().and_then(|p| p.get("session"))),
        })
    });
    let response = app.handle(method, uri, headers, raw).await;
    if let (Some(journal), Some(mut record)) = (&app.routes_journal, journal) {
        // Route and option NAMES only; never authorization or session values.
        record["status"] = json!(response.status().as_u16());
        record["route"] = json!(
            response
                .headers()
                .get("x-readyset-route")
                .and_then(|v| v.to_str().ok())
        );
        journal.write(&record);
    }
    response
}

/// Oracle round: whole bounded batch, full replica re-verification (POC regressions).
async fn oracle_round(app: &App, replicator: &mut Replicator) -> Result<Value> {
    // BEGIN + consuming read; the Stream advances only when this capture commits.
    let prepared = replicator.begin_capture().await?;
    let witnessed = replicator.witness(&prepared);
    let applied = match witnessed {
        Ok((rows, missing)) => app
            .engine
            .legacy()?
            .apply_and_barrier(&prepared.batch, rows, missing)
            .await
            .map(|_| ()),
        Err(error) => Err(error),
    };
    if let Err(error) = applied {
        // Not applied: roll back so the changes stay in the Stream, then fail closed.
        let error = replicator.abort_capture(error).await;
        app.fail(format!("engine apply/barrier failure: {error:#}"));
        return Err(error);
    }
    let changes = prepared.batch.row_count;
    // Applied but not committed is uncertain for the Stream; never retried (fail closed).
    replicator.commit_capture(prepared).await?;
    Ok(json!({ "changes": changes }))
}

/// Read every page of the consuming read into one batch writer.
async fn consume_pages(
    pages: &mut Pages<'_>,
    engine: &Arc<Engine>,
    sequence: u64,
) -> Result<(BatchWriter, usize)> {
    let first = pages.next().await?;
    // More than one page: reads go upstream instead of waiting for this batch.
    let mut writer = engine.begin_batch(sequence, pages.has_more()).await?;
    let mut changes = 0;
    let mut page = first;
    while let Some(rows) = page {
        changes += rows.len();
        writer.push(parse_changes(&rows)?).await?;
        page = pages.next().await?;
    }
    Ok((writer, changes))
}

/// BEGIN plus the whole consuming read, applied chunk by chunk. Only a read that reached
/// `Succeeded` with every page returned yields a writer that may be committed.
async fn read_batch(
    replicator: &mut Replicator,
    engine: &Arc<Engine>,
    sequence: u64,
) -> Result<(BatchWriter, usize)> {
    let mut pages = replicator.begin_consume().await?;
    match consume_pages(&mut pages, engine, sequence).await {
        Ok((writer, changes)) => match pages.close().await {
            Ok(_) => Ok((writer, changes)),
            Err(error) => {
                drop(writer);
                Err(error)
            }
        },
        // A writer dropped mid-batch poisons the engine; the Stream keeps every change.
        Err(error) => Err(pages.abort(error).await),
    }
}

/// Production round: stream the whole Stream into ReadySet in chunks, then COMMIT.
async fn stream_round(app: &App, replicator: &mut Replicator) -> Result<Value> {
    let started = Instant::now();
    let sequence = replicator.sequence + 1;
    let read = read_batch(replicator, app.engine.legacy()?, sequence).await;
    let (writer, changes) = match read {
        Ok(read) => read,
        Err(error) => return Err(replicator.abort_capture(error).await),
    };
    let read_ms = started.elapsed().as_secs_f64() * 1000.0;
    let (inserts, deletes, cancelled) = (writer.inserts, writer.deletes, writer.cancelled);
    let finished = match writer.finish().await {
        Ok(finished) => finished,
        Err(error) => return Err(replicator.abort_capture(error).await),
    };
    let committing = Instant::now();
    replicator.commit_consumed(inserts, deletes).await?;
    let total = started.elapsed().as_secs_f64();
    let summary = json!({
        "sequence": sequence, "changes": changes, "inserts": inserts, "deletes": deletes,
        "cancelled": cancelled, "read_apply_ms": read_ms, "finish": finished,
        "commit_ms": committing.elapsed().as_secs_f64() * 1000.0, "total_ms": total * 1000.0,
        "changes_per_sec": if total > 0.0 { changes as f64 / total } else { 0.0 },
    });
    if changes > 0 {
        eprintln!("batch {summary}");
    }
    Ok(summary)
}

async fn sync_once(app: &App, replicator: &mut Replicator) -> Result<()> {
    let _capturing = app.capturing.lock().await;
    let capture_started = replicator.bootstrap_started().unwrap_or_else(Instant::now);
    let (epoch, clean) = {
        let st = app.lock();
        (st.forward_epoch, st.active_forwards == 0)
    };
    let summary = if app.s.oracle {
        oracle_round(app, replicator).await?
    } else {
        stream_round(app, replicator).await?
    };
    {
        let mut st = app.lock();
        st.sequence = replicator.sequence;
        st.rows = replicator.rows;
        st.last_sync = Some(capture_started);
        st.counters.sync_batches += 1;
        if summary["changes"].as_u64().unwrap_or(0) > 0 {
            st.last_batch = summary;
        }
        // Ready only if no forwarded statement could have changed the source after this
        // capture began, or was still running when it began.
        st.not_ready = if !clean || st.forward_epoch != epoch {
            Some("forward_during_capture")
        } else if !st.inflight.is_empty() {
            Some("upstream_inflight")
        } else if st.uncertain {
            Some("uncertain_upstream")
        } else if capture_started.elapsed() > app.s.max_staleness {
            Some("capture_slow")
        } else {
            None
        };
        st.ready = st.not_ready.is_none();
    }
    app.write_status("status.json");
    Ok(())
}

async fn sync_loop(app: Arc<App>, mut replicator: Replicator, mut stop: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(app.s.interval) => {}
            _ = stop.changed() => {}
        }
        if *stop.borrow() {
            break;
        }
        if let Err(error) = sync_once(&app, &mut replicator).await {
            let reason = format!("sync failure: {error:#}");
            let _ = fs::write(
                app.s.run_dir.join("sync_failure.txt"),
                format!("{reason}\n"),
            );
            // Uncertain outcomes are not blindly retried. Upstream passthrough stays available.
            app.fail(reason);
            break;
        }
    }
}

async fn shutdown_signal() {
    let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(term) => term,
        Err(_) => return std::future::pending().await,
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(test)]
#[path = "server/config_tests.rs"]
mod config_tests;
#[cfg(test)]
#[path = "server/lifecycle_tests.rs"]
mod lifecycle_tests;
#[path = "server/shared.rs"]
mod shared_http;
use shared_http::run_shared;

async fn run(s: Settings) -> Result<()> {
    if !s.tables.is_empty() {
        return run_shared(s).await;
    }
    fs::create_dir_all(&s.run_dir)?;
    let journal = Journal::open(&s.run_dir.join("source_queries.jsonl"))?;
    let http = http_client()?;
    let auth = basic_auth(&s.user, &s.password);
    let generation = format!("rs_{}_{}", s.instance_id, new_id(12));
    let names = Names::new(
        &s.declaration.database,
        &s.declaration.table,
        &s.instance_id,
        &generation,
    );
    let client = |label: &str| {
        DatabendClient::new(
            http.clone(),
            &s.endpoint,
            &auth,
            label,
            Some(Arc::clone(&journal)),
        )
    };
    let mut replicator = Replicator::new(
        names.clone(),
        client("bootstrap"),
        client("capture"),
        s.oracle,
    );
    replicator.prepare().await?;
    let started = Instant::now();
    let mut loader = Engine::loader(EngineConfig {
        generation: generation.clone(),
        storage_dir: s.run_dir.join("engine").join("storage"),
        declaration: Some(s.declaration.clone()),
        oracle: s.oracle,
        cache: s.cache,
    })
    .await?;
    let rows = replicator.snapshot(&mut loader).await?;
    let loaded_secs = started.elapsed().as_secs_f64();
    let engine = Arc::new(loader.finish(rows).await?);
    let snapshot = json!({
        "rows": replicator.rows, "read_load_secs": loaded_secs,
        "total_secs": started.elapsed().as_secs_f64(),
        "rows_per_sec": replicator.rows as f64 / loaded_secs.max(1e-9),
    });
    eprintln!(
        "snapshot {} rows from {}: {snapshot}",
        replicator.rows, names.source
    );
    let (stop, stop_rx) = watch::channel(false);
    let routes_journal = if s.route_journal {
        Some(Journal::open(&s.run_dir.join("routes.jsonl"))?)
    } else {
        None
    };
    let state = RouteState {
        snapshot,
        ..RouteState::default()
    };
    let app = Arc::new(App {
        auth,
        http,
        engine: CacheEngine::Legacy(Arc::clone(&engine)),
        tables: Default::default(),
        names: names.clone(),
        default_role: StdMutex::new(replicator.default_role.clone()),
        state: StdMutex::new(state),
        results: StdMutex::new(HashMap::new()),
        finalized: StdMutex::new(Finalized::default()),
        drained: StdMutex::new(Drained::default()),
        passive: StdMutex::new(HashMap::new()),
        control: tokio::sync::Mutex::new(()),
        capturing: tokio::sync::Mutex::new(()),
        routes_journal,
        events: Journal::open(&s.run_dir.join("events.jsonl"))?,
        stop,
        s,
    });
    if let Err(error) = sync_once(&app, &mut replicator).await {
        engine.shutdown().await;
        return Err(error.context("initial capture failed"));
    }
    let worker = tokio::spawn(sync_loop(Arc::clone(&app), replicator, stop_rx));
    serve(app, vec![worker]).await
}

async fn serve(app: Arc<App>, workers: Vec<tokio::task::JoinHandle<()>>) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(app.s.listen).await?;
    let bound = listener.local_addr()?;
    let host = if bound.ip().is_unspecified() {
        "127.0.0.1".to_owned()
    } else {
        bound.ip().to_string()
    };
    let info = json!({
        "origin": format!("http://{host}:{}", bound.port()),
        "listen": bound.to_string(),
        "database": app.s.declaration.database,
        "instance_id": app.s.instance_id,
        "generation": app.names.generation,
        "stream": app.names.stream,
        "tables": app.tables.keys().collect::<Vec<_>>(),
    });
    fs::write(app.s.run_dir.join("listening.json"), format!("{info:#}\n"))?;
    println!("{info}");
    app.event("listening", None, info.clone());
    let router = axum::Router::new()
        .fallback(dispatch)
        .with_state(Arc::clone(&app));
    let (signal_tx, signal_rx) = watch::channel(false);
    let watched = Arc::clone(&app);
    tokio::spawn(async move {
        tokio::select! {
            _ = shutdown_signal() => {}
            // Shared runtime: a global fail-closed (uncertain graph change) has no in-process
            // recovery. Exit so the supervisor restarts on the persisted state (seconds).
            _ = async {
                loop {
                    if matches!(&watched.engine, CacheEngine::Shared(e) if e.is_failed()) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            } => eprintln!("shared native engine failed closed; shutting down for restart"),
        }
        let _ = signal_tx.send(true);
    });
    let graceful = {
        let mut rx = signal_rx.clone();
        async move {
            let _ = rx.wait_for(|signaled| *signaled).await;
        }
    };
    let server = axum::serve(listener, router).with_graceful_shutdown(graceful);
    // Idle keep-alive clients must not hold the process open forever after a signal.
    let mut deadline_rx = signal_rx;
    let served = tokio::select! {
        served = async { server.await } => served,
        _ = async {
            let _ = deadline_rx.wait_for(|signaled| *signaled).await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        } => Ok(()),
    };
    // Stop capture, then the engine; record the final state either way.
    let _ = app.stop.send(true);
    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    for mut worker in workers {
        if tokio::time::timeout_at(until, &mut worker).await.is_err() {
            worker.abort();
            let _ = worker.await;
        }
    }
    app.engine.shutdown().await;
    app.write_status("final_status.json");
    let failed_closed = matches!(&app.engine, CacheEngine::Shared(e) if e.is_failed());
    app.event(
        "stopped",
        None,
        json!({"failed_closed": failed_closed, "serve_error": served.as_ref().err().map(|e| e.to_string())}),
    );
    served?;
    if failed_closed {
        bail!("shared native engine failed closed; restart required");
    }
    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let settings = settings(Cli::parse())?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(settings))
}
