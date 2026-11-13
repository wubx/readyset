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
use readyset_databend::engine::{BatchWriter, Engine, EngineConfig, ReadOutcome};
use readyset_databend::replicator::{Names, Replicator, parse_changes};
use readyset_databend::upstream::{
    DatabendClient, Journal, MAX_RESPONSE, Pages, basic_auth, bounded_body, http_client,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::sync::watch;

use readyset_alloc as _;

const MAX_BODY: usize = 1024 * 1024;
const MAX_LOCAL_QUERY_BYTES: usize = 8 * 1024 * 1024;
const MAX_LOCAL_TOTAL_BYTES: usize = 64 * 1024 * 1024;
const CONTROL_RESPONSE_RESERVE: usize = 2 * 1024 * 1024;
const MAX_LOCAL_RESULTS: usize = 2048;
const MAX_INFLIGHT: usize = 2048;
const LOCAL_RESULT_TTL: Duration = Duration::from_secs(60);

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
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageSection {
    dir: Option<PathBuf>,
}

struct Settings {
    endpoint: String,
    user: String,
    password: String,
    declaration: Declaration,
    run_dir: PathBuf,
    instance_id: String,
    listen: SocketAddr,
    interval: Duration,
    max_staleness: Duration,
    route_journal: bool,
    fault_injection: bool,
    oracle: bool,
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
    if declaration.database.is_empty() {
        let (database, table) = match (cli.database, cli.table) {
            (Some(d), Some(t)) => (d, t),
            (None, None) => {
                let tables = replication.tables.clone().unwrap_or_default();
                ensure!(tables.len() == 1, "phase 1 replicates exactly one table");
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
    let run_dir = match (cli.output, &storage) {
        (Some(output), _) => output,
        (None, Some(dir)) => {
            fs::create_dir_all(dir)?;
            dir.join("runs").join(format!(
                "{}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_secs(),
                new_id(6)
            ))
        }
        (None, None) => bail!("--output or [storage].dir required"),
    };
    ensure!(
        !run_dir.exists(),
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
    let instance_id = instance_id(file.instance_id, storage.as_deref())?;
    Ok(Settings {
        endpoint: endpoint.trim_end_matches('/').to_owned(),
        user,
        password: std::env::var("DATABEND_PASSWORD").unwrap_or_default(),
        declaration,
        run_dir,
        instance_id,
        listen,
        interval,
        max_staleness,
        route_journal: cli.route_journal,
        fault_injection: cli.enable_fault_injection,
        oracle: cli.oracle,
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
}

impl RouteState {
    fn available(&self, now: Instant, max_staleness: Duration) -> bool {
        self.ready
            && self.failed.is_none()
            && !self.uncertain
            && self.inflight.is_empty()
            && self.active_forwards == 0
            && self
                .last_sync
                .is_some_and(|t| now.saturating_duration_since(t) <= max_staleness)
    }
}

struct Stored {
    base: Map<String, Value>,
    rows: Vec<Value>,
    page_size: usize,
    bytes: usize,
}

type LocalResults = HashMap<String, (Instant, Option<Stored>)>;

struct App {
    s: Settings,
    auth: String,
    http: reqwest::Client,
    engine: Arc<Engine>,
    names: Names,
    default_role: Option<String>,
    state: StdMutex<RouteState>,
    results: StdMutex<LocalResults>,
    /// Cache DDL is serialized, like the gateway's single request lock.
    control: tokio::sync::Mutex<()>,
    /// Held for a whole capture. `/health` waits on it, so a reported checkpoint covers every
    /// capture that had already started (the gateway's serialized /health contract).
    capturing: tokio::sync::Mutex<()>,
    routes_journal: Option<Arc<Journal>>,
    stop: watch::Sender<bool>,
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
        h.insert(k, v);
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
    const ALLOWED: [&str; 8] = [
        "database",
        "catalog",
        "settings",
        "txn_state",
        "need_sticky",
        "need_keep_alive",
        "role",
        "internal",
    ];
    if s.keys().any(|k| !ALLOWED.contains(&k.as_str())) {
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
        && s.get("txn_state").is_none_or(|t| t == "AutoCommit")
        && s.get("need_sticky") != Some(&Value::Bool(true))
        && s.get("need_keep_alive") != Some(&Value::Bool(true))
        && s.get("database").is_none_or(Value::is_string)
        && pagination.keys().all(|k| {
            matches!(
                k.as_str(),
                "wait_time_secs" | "max_rows_per_page" | "max_rows_in_buffer"
            )
        })
        && pagination.values().all(small_int)
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

    fn status(&self) -> Value {
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
            st.failed.get_or_insert(reason);
        }
        let _ = self.stop.send(true);
    }

    fn local_bytes(results: &LocalResults) -> usize {
        // Budget retained JSON payload, not a claim about exact heap usage.
        results
            .values()
            .filter_map(|(_, s)| s.as_ref().map(|s| s.bytes))
            .sum()
    }

    fn local_query_id(headers: &HeaderMap, results: &LocalResults) -> Result<String, String> {
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
        if results.contains_key(&qid) {
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
                .max(1),
            bytes: size,
        };
        let page = local_page(&stored, &prefix, 0);
        results.insert(qid, (Instant::now() + LOCAL_RESULT_TTL, Some(stored)));
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
        if path == "/__fault/engine" && method == Method::POST && self.s.fault_injection {
            self.fail("injected engine failure".into());
            return reply(StatusCode::OK, r#"{"ok":true}"#, vec![]);
        }
        let start = path == "/v1/query" && method == Method::POST;
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
            if let Some((id, kind)) = &cycle
                && (results.contains_key(id) || id.starts_with("rs_local_"))
            {
                return self.local_lifecycle(&mut results, id, kind);
            }
        }
        let sql = body.get("sql").and_then(Value::as_str).unwrap_or_default();
        let database = session_database(&body);
        let sane = safe_session(&body, self.default_role.as_deref())
            && safe_headers(&headers)
            && (database == "default" || database == self.s.declaration.database);
        if start && cache_control_sql(sql) {
            return self
                .cache_control(&body, &headers, sql, &database, sane)
                .await;
        }
        let eligible = start
            && sane
            && {
                let st = self.lock();
                st.available(now, self.s.max_staleness)
            }
            && self.results().len() < MAX_LOCAL_RESULTS;
        if eligible {
            match self.engine.query(sql, &database).await {
                Ok(ReadOutcome::Matched {
                    template,
                    schema,
                    data,
                }) => {
                    let mut results = self.results();
                    let qid = match Self::local_query_id(&headers, &results) {
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
                }
                Ok(ReadOutcome::LimitMiss | ReadOutcome::NotMatched) => {}
                // Do not send further engine RPCs after an uncertain engine result.
                Err(error) => self.fail(format!("engine query failure: {error:#}")),
            }
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
        let Some((expires, Some(stored))) = results.get(id) else {
            return not_found("local query expired or finalized");
        };
        let expires = *expires;
        let prefix = format!("/v1/query/{id}");
        let page = match kind {
            Lifecycle::Page(number) => {
                if number.len() > 10 {
                    return not_found("local page does not exist");
                }
                let Ok(page) = number.parse::<usize>() else {
                    return not_found("local page does not exist");
                };
                if number != &page.to_string()
                    || page < 1
                    || page * stored.page_size >= stored.rows.len()
                {
                    return not_found("local page does not exist");
                }
                page
            }
            _ => 0,
        };
        let mut data = local_page(stored, &prefix, page);
        if matches!(kind, Lifecycle::Final | Lifecycle::Kill) {
            // Tombstone: a finalized local ID is never forwarded upstream.
            results.insert(id.to_owned(), (expires, None));
            data["data"] = json!([]);
            data["next_uri"] = Value::Null;
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
        let unavailable = {
            let st = self.lock();
            !st.available(Instant::now(), self.s.max_staleness)
        } || {
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
        let qid = match Self::local_query_id(headers, &self.results()) {
            Ok(qid) => qid,
            Err(e) => return reply(StatusCode::BAD_REQUEST, error_body(&e), vec![]),
        };
        let failure = match self.engine.cache_ddl(sql, database).await {
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
            if name.as_str().starts_with("x-databend-") {
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
            .filter(|(k, _)| k.as_str().starts_with("x-databend-") || *k == header::CONTENT_TYPE)
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
        let ready = app.lock().available(Instant::now(), app.s.max_staleness);
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
    let read = read_batch(replicator, &app.engine, sequence).await;
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
    let capture_started = Instant::now();
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
        st.ready = clean
            && st.forward_epoch == epoch
            && st.inflight.is_empty()
            && !st.uncertain
            && capture_started.elapsed() <= app.s.max_staleness;
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

async fn run(s: Settings) -> Result<()> {
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
        engine: Arc::clone(&engine),
        names: names.clone(),
        default_role: replicator.default_role.clone(),
        state: StdMutex::new(state),
        results: StdMutex::new(HashMap::new()),
        control: tokio::sync::Mutex::new(()),
        capturing: tokio::sync::Mutex::new(()),
        routes_journal,
        stop,
        s,
    });
    if let Err(error) = sync_once(&app, &mut replicator).await {
        engine.shutdown().await;
        return Err(error.context("initial capture failed"));
    }
    let worker = tokio::spawn(sync_loop(Arc::clone(&app), replicator, stop_rx));
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
        "generation": names.generation,
        "stream": names.stream,
    });
    fs::write(app.s.run_dir.join("listening.json"), format!("{info:#}\n"))?;
    println!("{info}");
    let router = axum::Router::new()
        .fallback(dispatch)
        .with_state(Arc::clone(&app));
    let (signal_tx, signal_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
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
    let _ = tokio::time::timeout(Duration::from_secs(60), worker).await;
    engine.shutdown().await;
    app.write_status("final_status.json");
    served?;
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
