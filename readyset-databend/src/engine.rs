//! In-process ReadySet engine for the Databend single table: base table, declared caches,
//! fenced replication barrier and native reads. Shared by `databend-m2` (stdin protocol) and
//! the `readyset-databend` single binary (HTTP), so both run identical ReadySet logic.
//!
//! Reads use the server's shared `Readers` map through `ReadRequestHandler` (the standalone
//! adapter's in-process path). The reader RPC listener still runs for the barrier oracle.
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use readyset_client::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use readyset_client::recipe::changelist::{Change, ChangeList};
use readyset_client::{ReaderHandle, ReadySetHandle, Table, TableOperation, View};
use readyset_data::{DfValue, Dialect};
use readyset_server::worker::readers::{ReadRequestHandler, retry_misses};
use readyset_server::{Builder, DurabilityMode, Handle, PersistenceParameters, Readers};
use readyset_sql::ast::TrxCachePolicy;
use readyset_util::shutdown::ShutdownSender;
use replication_offset::ReplicationOffset;
use replication_offset::databend::DatabendPosition;
use serde_json::{Value, json};

use crate::cache_control::{self, CacheControl, INTERNAL_QUERY};
use crate::declaration::{Declaration, Template};
use crate::native_query::{self, LOCAL_TABLE, NativeQuery};
use crate::{Action, Batch, Change as SourceChange, Sink, SourceRow, delivery, reader_fence};

pub struct EngineConfig {
    pub generation: String,
    pub storage_dir: PathBuf,
    /// Source binding plus optional bootstrap templates. `None` only for raw M2 experiments.
    pub declaration: Option<Declaration>,
    /// `true`: POC content oracle. The engine keeps every source row and re-verifies each
    /// reader after every batch (bounded tables only; `databend-m2` and the regression
    /// verifiers). `false`: production mode. No per-batch work proportional to table size;
    /// each batch is fenced with one marker row instead.
    pub oracle: bool,
}

/// Oracle mode keeps every row in memory and re-verifies all of them per batch.
pub const ORACLE_MAX_ROWS: usize = 10000;
/// A huge batch can keep readers busy for minutes; the fence waits that long, then fails.
const FENCE_TIMEOUT: Duration = Duration::from_secs(1800);
const LOAD_CHUNK: usize = 10000;

/// A published, fenced cache route. Reads clone the view; the template is immutable.
pub struct Route {
    pub template: Arc<Template>,
    pub view: View,
}

pub enum ReadOutcome {
    Matched {
        template: String,
        schema: Vec<Value>,
        data: Vec<Vec<Value>>,
    },
    /// Native result exceeded the local row/byte budget: serve from upstream instead.
    LimitMiss,
    NotMatched,
}

struct DeclaredReader {
    view: View,
    /// Every previously witnessed physical key is checked again, including keys vacated by
    /// UPDATE/DELETE. This is an oracle only, never application key construction.
    witnessed: HashSet<Vec<DfValue>>,
}

struct Core {
    handle: Handle,
    shutdown: Option<ShutdownSender>,
    table: Table,
    sink: Sink,
    reader: ReaderHandle,
    /// The internal id reader as a `View`, for marker fences.
    internal_view: View,
    internal: NativeQuery,
    templates: Vec<Arc<Template>>,
    declared: Vec<DeclaredReader>,
    oracle: bool,
    /// Oracle mode only: every source row, re-verified after each batch.
    barrier_rows: Vec<SourceRow>,
    barrier_missing: Vec<i64>,
    barrier_checkpoint: u64,
    generation: String,
}

pub struct Engine {
    core: Arc<tokio::sync::Mutex<Core>>,
    /// Readers hold it shared; applying a batch + its barrier, or dropping a route, hold it
    /// exclusively, so no read observes a half-applied batch or a removed reader.
    gate: Arc<tokio::sync::RwLock<()>>,
    /// A multi-page batch is being applied: reads do not wait for the gate, they go upstream.
    large_batch: std::sync::atomic::AtomicBool,
    routes: StdRwLock<Arc<Vec<Arc<Route>>>>,
    /// In-process read handlers are Send but not Sync: one per concurrent read, pooled.
    handlers: StdMutex<(ReadRequestHandler, Vec<ReadRequestHandler>)>,
    declaration: Option<Declaration>,
    /// Set after any uncertain engine failure. No further engine RPC is attempted.
    poisoned: std::sync::atomic::AtomicBool,
}

async fn wait_rows(reader: &mut ReaderHandle, count: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if reader.len().await? == count {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("reader row-count convergence timeout")?
}

async fn verify_reader(
    reader: &mut ReaderHandle,
    label: String,
    rows: &[SourceRow],
    missing: &[i64],
) -> Result<Value> {
    ensure!(
        rows.len() + missing.len() <= 20000,
        "verification request too large"
    );
    let ids: HashSet<_> = rows.iter().map(|r| r.id).collect();
    ensure!(ids.len() == rows.len(), "expected source keys not unique");
    ensure!(
        missing.iter().all(|id| !ids.contains(id)),
        "conflicting expected/missing keys"
    );
    let started = Instant::now();
    // ReaderHandle::len counts key slots; deleted keys may retain empty slots.
    // Validate live contents instead of equating that size to the source row count.
    for (id, expected) in rows
        .iter()
        .map(|r| (r.id, vec![r.values()]))
        .chain(missing.iter().map(|&id| (id, vec![])))
    {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let actual = reader
                    .lookup(&[id.into()], Dialect::DEFAULT_MYSQL)
                    .await?
                    .into_vec();
                if actual == expected {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .with_context(|| format!("query result mismatch for {label}, key {id}"))??;
    }
    let keys = reader.keys().await?;
    let mut extra_keys_checked = 0;
    for key in &keys {
        ensure!(key.len() == 1, "unexpected composite reader key");
        let id = i64::try_from(key[0].clone())?;
        if !ids.contains(&id) {
            let actual = reader.lookup(key, Dialect::DEFAULT_MYSQL).await?.into_vec();
            ensure!(
                actual.is_empty(),
                "unexpected extra live row at key {id}: {actual:?}"
            );
            extra_keys_checked += 1;
        }
    }
    Ok(
        json!({"ok":true,"label":label,"rows_verified":rows.len(),"missing_verified":missing.len(),
              "live_rows_verified":rows.len(),"reader_key_slots":reader.len().await?,
              "extra_keys_checked":extra_keys_checked,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0}),
    )
}

fn internal_query(declaration: &Declaration) -> Result<NativeQuery> {
    Ok(declaration
        .template_for_sql(
            "barrier".into(),
            &format!(
                "SELECT id,payload,score,note FROM {}.{} WHERE id=?",
                declaration.database, declaration.table
            ),
            &declaration.database,
            LOCAL_TABLE,
        )?
        .query)
}

async fn create_reader(
    handle: &mut Handle,
    template: &Template,
    internal: &NativeQuery,
) -> Result<View> {
    // Compare FULL native query identity, not merely its projection. All-column score lookups
    // must never alias the protected id barrier. ReadySet DROP removes identical-query aliases.
    let name = if template.query.same_shape(internal) {
        INTERNAL_QUERY
    } else {
        handle
            .extend_recipe(ChangeList::from_change(
                Change::create_cache(
                    template.name.clone(),
                    template.query.statement.clone(),
                    TrxCachePolicy::Never,
                    None,
                    None,
                ),
                Dialect::DEFAULT_MYSQL,
            ))
            .await?;
        template.name.as_str()
    };
    let view = handle.view(name).await?;
    ensure!(
        matches!(&view, View::Single(_)),
        "single declared reader required"
    );
    ensure!(
        handle
            .materialization_info()
            .await?
            .iter()
            .all(|m| !m.partial),
        "partial query state"
    );
    Ok(view)
}

async fn fence_template(
    template: &Template,
    reader: &mut DeclaredReader,
    rows: &[SourceRow],
) -> Result<()> {
    let mut expected: HashMap<Vec<DfValue>, Vec<Vec<DfValue>>> = HashMap::new();
    for row in rows {
        let key = reader_fence::physical_key(&reader.view, row)?;
        // Equality/IN NULL predicates are not admitted; native point lookups skip NULL keys.
        if !key.iter().any(DfValue::is_none) {
            expected
                .entry(key)
                .or_default()
                .push(template.query.project(row));
        }
    }
    for bag in expected.values_mut() {
        bag.sort();
    }
    reader.witnessed.extend(expected.keys().cloned());
    // Inspect all comparable physical keys, including stale extras. NULL predicate slots
    // remain unadmitted. No SQL predicate evaluator or inverse parameter mapping is needed.
    let View::Single(handle) = &mut reader.view else {
        bail!("single reader required")
    };
    reader.witnessed.extend(
        handle
            .keys()
            .await?
            .into_iter()
            .filter(|key| !key.iter().any(DfValue::is_none)),
    );
    ensure!(
        reader.witnessed.len() <= 20000,
        "POC oracle key history exceeds 20000; restart required"
    );
    for params in &reader.witnessed {
        let wanted = expected.get(params).cloned().unwrap_or_default();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut actual = reader_fence::point_contents(&mut reader.view, params).await?;
                actual.sort();
                if actual == wanted {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .with_context(|| format!("declared reader barrier timeout: {}", template.name))??;
    }
    Ok(())
}

fn cache_listing(templates: &[Arc<Template>]) -> Value {
    json!({"ok":true,"schema":[{"name":"name","type":"String"},
        {"name":"query","type":"String"},{"name":"state","type":"String"}],
        "data":templates.iter().map(|t| vec![t.name.clone(),t.source_sql.clone(),"ready".into()]).collect::<Vec<_>>(),
        "caches":templates.iter().map(|t| t.name.clone()).collect::<Vec<_>>()})
}

fn build_routes(templates: &[Arc<Template>], declared: &[DeclaredReader]) -> Vec<Arc<Route>> {
    templates
        .iter()
        .zip(declared)
        .map(|(template, reader)| {
            Arc::new(Route {
                template: Arc::clone(template),
                view: reader.view.clone(),
            })
        })
        .collect()
}

impl Core {
    async fn apply(&mut self, batch: &Batch) -> Result<Value> {
        let current = self.sink.checkpoint(&mut self.handle).await?;
        // Only preflight failures are recoverable. An uncertain Table RPC/persistence error is
        // returned as Err (fatal); it is never blindly retried.
        match delivery(&current, batch) {
            Err(error) => Ok(
                json!({"ok":false,"rejected":true,"error":format!("{error:#}"),"checkpoint":current.sequence()}),
            ),
            Ok(_) => {
                self.sink
                    .apply(&mut self.handle, &mut self.table, batch)
                    .await
            }
        }
    }

    async fn barrier(&mut self, rows: Vec<SourceRow>, missing: Vec<i64>) -> Result<Value> {
        let result = verify_reader(
            &mut self.reader,
            "replication barrier".into(),
            &rows,
            &missing,
        )
        .await?;
        // Keep a fenced oracle for runtime cache construction. Never rescan the source.
        for (t, r) in self.templates.iter().zip(&mut self.declared) {
            fence_template(t, r, &rows).await?;
        }
        self.barrier_rows = rows;
        self.barrier_missing = missing;
        self.barrier_checkpoint = self.sink.checkpoint(&mut self.handle).await?.sequence();
        Ok(result)
    }
}

/// What a reader stores for one source row.
#[derive(Clone)]
enum Projection {
    /// The internal id reader: all four columns.
    Full,
    Template(Arc<Template>),
}

impl Projection {
    fn apply(&self, row: &SourceRow) -> Vec<DfValue> {
        match self {
            Projection::Full => row.values(),
            Projection::Template(t) => t.query.project(row),
        }
    }
}

fn fence_targets(core: &Core) -> Vec<(View, Projection)> {
    std::iter::once((core.internal_view.clone(), Projection::Full))
        .chain(
            core.templates
                .iter()
                .zip(&core.declared)
                .map(|(t, r)| (r.view.clone(), Projection::Template(Arc::clone(t)))),
        )
        .collect()
}

async fn copies(view: &mut View, key: &[DfValue], wanted: &[DfValue]) -> Result<usize> {
    Ok(reader_fence::point_contents(view, key)
        .await?
        .iter()
        .filter(|row| row.as_slice() == wanted)
        .count())
}

/// A reader still replaying its initial contents (or busy with a huge batch) answers lookups
/// with RPC timeouts. Retry until `until`; report the last error if it never answers.
async fn copies_retrying(
    view: &mut View,
    key: &[DfValue],
    wanted: &[DfValue],
    until: Instant,
) -> Result<usize> {
    loop {
        match copies(view, key, wanted).await {
            Ok(n) => return Ok(n),
            Err(_) if Instant::now() < until => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(error.context("marker fence: reader never answered")),
        }
    }
}

/// Wait without excluding application reads until a (new) reader answers lookups at all,
/// i.e. it finished its initial replay. Only then is a short read-excluding fence needed.
async fn wait_readable(view: &mut View, projection: &Projection) -> Result<()> {
    let probe = SourceRow {
        id: 0,
        payload: String::new(),
        score: 0,
        note: Some(String::new()),
    };
    let key = reader_fence::physical_key(view, &probe)?;
    copies_retrying(
        view,
        &key,
        &projection.apply(&probe),
        Instant::now() + FENCE_TIMEOUT,
    )
    .await
    .map(|_| ())
}

async fn wait_copies(
    targets: &mut [(View, Projection)],
    probes: &[(Vec<DfValue>, Vec<DfValue>, usize)],
    added: usize,
    until: Instant,
) -> Result<()> {
    for ((view, _), (key, wanted, before)) in targets.iter_mut().zip(probes) {
        while copies_retrying(view, key, wanted, until).await? != before + added {
            ensure!(Instant::now() < until, "marker fence timeout");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    Ok(())
}

/// Production barrier, O(readers) instead of O(rows). Every reader fully materializes every
/// base row (plain projections with parameter-only predicates), and each reader applies base
/// updates in order. So once a marker row written AFTER the batch is visible in a reader, and
/// then gone again, that reader reflects the whole batch. Readers that the caller is
/// publishing must be excluded from application reads while this runs (the marker is a real
/// base row for a moment). Copies are counted, so a marker equal to real data is harmless.
async fn marker_fence(table: &mut Table, targets: &mut [(View, Projection)]) -> Result<()> {
    let nonce = uuid::Uuid::new_v4();
    let bits = nonce.as_u128();
    let text = format!("readyset-fence-{nonce}");
    let marker = SourceRow {
        id: bits as i64,
        payload: text.clone(),
        score: (bits >> 64) as i64,
        note: Some(text),
    };
    let until = Instant::now() + FENCE_TIMEOUT;
    let mut probes = Vec::with_capacity(targets.len());
    for (view, projection) in targets.iter_mut() {
        let key = reader_fence::physical_key(view, &marker)?;
        let wanted = projection.apply(&marker);
        let before = copies_retrying(view, &key, &wanted, until).await?;
        probes.push((key, wanted, before));
    }
    table
        .perform_all(vec![TableOperation::Insert(marker.values())])
        .await?;
    wait_copies(targets, &probes, 1, until).await?;
    table
        .perform_all(vec![TableOperation::DeleteRow {
            row: marker.values(),
        }])
        .await?;
    wait_copies(targets, &probes, 0, until).await
}

/// Bulk snapshot loader returned by [`Engine::loader`].
pub struct Loader {
    handle: Handle,
    shutdown: ShutdownSender,
    table: Table,
    handler: ReadRequestHandler,
    config: EngineConfig,
    loaded: usize,
}

impl Loader {
    pub async fn load(&mut self, rows: &[SourceRow]) -> Result<()> {
        ensure!(
            !self.config.oracle || self.loaded + rows.len() <= ORACLE_MAX_ROWS,
            "oracle mode is limited to {ORACLE_MAX_ROWS} rows"
        );
        for chunk in rows.chunks(LOAD_CHUNK) {
            self.table
                .insert_many(chunk.iter().map(SourceRow::values))
                .await?;
        }
        self.loaded += rows.len();
        Ok(())
    }

    pub fn loaded(&self) -> usize {
        self.loaded
    }

    /// Checkpoint the snapshot, create the internal and declared readers, and fence them.
    /// Oracle mode needs the snapshot rows again (`rows`); production mode ignores them.
    pub async fn finish(self, rows: Option<Vec<SourceRow>>) -> Result<Engine> {
        let Loader {
            mut handle,
            shutdown,
            mut table,
            handler,
            config,
            loaded,
        } = self;
        let rows = if config.oracle {
            let rows = rows.context("oracle mode needs the snapshot rows")?;
            ensure!(
                !rows.is_empty() && rows.len() == loaded,
                "oracle mode needs a nonempty snapshot of exactly the loaded rows"
            );
            ensure!(
                rows.iter().map(|r| r.id).collect::<HashSet<_>>().len() == rows.len(),
                "duplicate initial key"
            );
            rows
        } else {
            Vec::new()
        };
        let initial = config
            .declaration
            .as_ref()
            .map(|d| d.templates(LOCAL_TABLE))
            .transpose()?
            .unwrap_or_default();
        let fallback = Declaration {
            database: "poc".into(),
            table: "source_rows".into(),
            queries: vec![],
        };
        let internal = internal_query(config.declaration.as_ref().unwrap_or(&fallback))?;
        table
            .set_replication_offset(ReplicationOffset::Databend(DatabendPosition::new(
                &config.generation,
                0,
            )?))
            .await?;
        let sink = Sink::new(config.generation.clone(), LOCAL_TABLE.into())?;
        sink.wait_persisted_for(&mut handle, 0, FENCE_TIMEOUT)
            .await?;
        handle
            .extend_recipe(ChangeList::from_change(
                Change::create_cache(
                    INTERNAL_QUERY,
                    internal.statement.clone(),
                    TrxCachePolicy::Never,
                    None,
                    None,
                ),
                Dialect::DEFAULT_MYSQL,
            ))
            .await?;
        let internal_view = handle.view(INTERNAL_QUERY).await?;
        let mut reader = handle
            .view(INTERNAL_QUERY)
            .await?
            .into_reader_handle()
            .context("single reader required")?;
        if config.oracle {
            wait_rows(&mut reader, rows.len()).await?;
        }
        ensure!(
            handle
                .materialization_info()
                .await?
                .iter()
                .all(|m| !m.partial),
            "partial query state"
        );
        let mut templates = Vec::new();
        let mut declared = Vec::new();
        for template in initial {
            let mut r = DeclaredReader {
                view: create_reader(&mut handle, &template, &internal).await?,
                witnessed: HashSet::new(),
            };
            if config.oracle {
                fence_template(&template, &mut r, &rows).await?;
            }
            templates.push(Arc::new(template));
            declared.push(r);
        }
        let routes = build_routes(&templates, &declared);
        let mut core = Core {
            handle,
            shutdown: Some(shutdown),
            table,
            sink,
            reader,
            internal_view,
            internal,
            templates,
            declared,
            oracle: config.oracle,
            barrier_rows: rows,
            barrier_missing: Vec::new(),
            barrier_checkpoint: 0,
            generation: config.generation,
        };
        if !core.oracle {
            // Nothing is published yet, so the marker cannot be read by an application.
            let mut targets = fence_targets(&core);
            marker_fence(&mut core.table, &mut targets).await?;
        }
        Ok(Engine {
            core: Arc::new(tokio::sync::Mutex::new(core)),
            gate: Arc::new(tokio::sync::RwLock::new(())),
            large_batch: std::sync::atomic::AtomicBool::new(false),
            routes: StdRwLock::new(Arc::new(routes)),
            handlers: StdMutex::new((handler, Vec::new())),
            declaration: config.declaration,
            poisoned: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

/// One production-mode batch, applied in chunks while reads are excluded (see
/// [`Engine::begin_batch`]). Dropping it without `finish` leaves the engine poisoned.
pub struct BatchWriter {
    engine: Arc<Engine>,
    core: tokio::sync::OwnedMutexGuard<Core>,
    _gate: tokio::sync::OwnedRwLockWriteGuard<()>,
    sequence: u64,
    finished: bool,
    pub inserts: u64,
    pub deletes: u64,
    /// DELETE/INSERT pairs of an identical full row (for example REPLACE INTO of an unchanged
    /// row) cancel out and are not sent to ReadySet.
    pub cancelled: u64,
}

impl BatchWriter {
    /// Apply one chunk of Stream changes. Bag semantics: each DELETE removes one copy of its
    /// full before-image row, so duplicate keys and any order across chunks are correct.
    pub async fn push(&mut self, changes: Vec<SourceChange>) -> Result<()> {
        let result = self.push_inner(changes).await;
        self.engine.guard(result)
    }

    async fn push_inner(&mut self, changes: Vec<SourceChange>) -> Result<()> {
        let mut deleted: HashMap<SourceRow, usize> = HashMap::new();
        for c in changes.iter().filter(|c| c.action == Action::Delete) {
            *deleted.entry(c.row.clone()).or_default() += 1;
        }
        let mut inserts = Vec::new();
        for c in changes.into_iter().filter(|c| c.action == Action::Insert) {
            match deleted.get_mut(&c.row) {
                Some(n) if *n > 0 => {
                    *n -= 1;
                    self.cancelled += 1;
                }
                _ => inserts.push(c.row),
            }
        }
        let mut operations = Vec::with_capacity(deleted.len() + inserts.len());
        for (row, n) in deleted {
            for _ in 0..n {
                operations.push(TableOperation::DeleteRow { row: row.values() });
            }
        }
        let deletes = operations.len() as u64;
        operations.extend(inserts.iter().map(|r| TableOperation::Insert(r.values())));
        for chunk in operations.chunks(LOAD_CHUNK) {
            self.core.table.perform_all(chunk.to_vec()).await?;
        }
        self.deletes += deletes;
        self.inserts += inserts.len() as u64;
        Ok(())
    }

    /// Checkpoint the batch, wait until it is persisted and every reader reflects it.
    pub async fn finish(mut self) -> Result<Value> {
        let result = self.finish_inner().await;
        self.finished = result.is_ok();
        self.engine.guard(result)
    }

    async fn finish_inner(&mut self) -> Result<Value> {
        let started = Instant::now();
        let core = &mut *self.core;
        core.table
            .perform_all(vec![TableOperation::SetReplicationOffset(
                ReplicationOffset::Databend(DatabendPosition::new(
                    &core.generation,
                    self.sequence,
                )?),
            )])
            .await?;
        core.sink
            .wait_persisted_for(&mut core.handle, self.sequence, FENCE_TIMEOUT)
            .await?;
        let persisted_ms = started.elapsed().as_secs_f64() * 1000.0;
        if self.inserts + self.deletes > 0 {
            let mut targets = fence_targets(core);
            marker_fence(&mut core.table, &mut targets).await?;
        }
        core.barrier_checkpoint = self.sequence;
        Ok(
            json!({"ok": true, "checkpoint": self.sequence, "inserts": self.inserts,
            "deletes": self.deletes, "cancelled": self.cancelled, "persisted_ms": persisted_ms,
            "finish_ms": started.elapsed().as_secs_f64() * 1000.0}),
        )
    }
}

impl Drop for BatchWriter {
    fn drop(&mut self) {
        if !self.finished {
            // Some chunks may be applied: the replica no longer matches any source version.
            self.engine.poison();
        }
        self.engine
            .large_batch
            .store(false, std::sync::atomic::Ordering::Release);
    }
}

impl Engine {
    /// Bounded start from an in-memory snapshot (`databend-m2`, oracle mode, tests).
    pub async fn start(config: EngineConfig, rows: Vec<SourceRow>) -> Result<Self> {
        let mut loader = Self::loader(config).await?;
        loader.load(&rows).await?;
        loader.finish(Some(rows)).await
    }

    /// Start the server and create the empty base table. Load the snapshot in chunks with
    /// [`Loader::load`], then call [`Loader::finish`].
    pub async fn loader(config: EngineConfig) -> Result<Loader> {
        DatabendPosition::new(&config.generation, 0)?;
        let mut builder = Builder::default();
        builder.set_external_addr("127.0.0.1:0".parse()?);
        builder.set_dialect(readyset_sql::Dialect::MySQL);
        builder.disable_partial();
        builder.set_mixed_comparisons(true);
        // No upstream URL; do not use replication_enabled=false (shallow-only deep-state wipe).
        builder.set_persistence(PersistenceParameters {
            mode: DurabilityMode::Permanent,
            db_filename_prefix: "m2".into(),
            storage_dir: Some(config.storage_dir.clone()),
            // Use the tracked background WAL-sync path. The synchronous (0) path currently
            // leaves the durability watermark at Unflushed after offset-bearing writes.
            wal_flush_interval_seconds: 1,
            ..Default::default()
        });
        let readers: Readers = Default::default();
        let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(Arc::new(
            LocalAuthorityStore::new(),
        ))));
        let (mut handle, shutdown) = builder
            .start_with_shared_readers(authority, Arc::clone(&readers))
            .await?;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(retry_misses(rx));
        let handler = ReadRequestHandler::new(readers, tx, Duration::from_secs(5));
        handle.backend_ready().await;
        handle
            .extend_recipe(ChangeList::from_strings(
                vec![native_query::LOCAL_DDL],
                Dialect::DEFAULT_MYSQL,
            )?)
            .await?;
        let table = handle.table(LOCAL_TABLE).await?;
        Ok(Loader {
            handle,
            shutdown,
            table,
            handler,
            config,
            loaded: 0,
        })
    }

    fn check_alive(&self) -> Result<()> {
        ensure!(
            !self.poisoned.load(std::sync::atomic::Ordering::Acquire),
            "engine is failed closed after an uncertain error"
        );
        Ok(())
    }

    /// Fail closed: after this no engine operation runs again (reads go upstream).
    pub fn poison(&self) {
        self.poisoned
            .store(true, std::sync::atomic::Ordering::Release);
    }

    fn guard<T>(&self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            self.poison();
        }
        result
    }

    fn publish(&self, core: &Core) {
        let routes = Arc::new(build_routes(&core.templates, &core.declared));
        *self.routes.write().unwrap_or_else(|e| e.into_inner()) = routes;
    }

    fn take_handler(&self) -> ReadRequestHandler {
        let mut pool = self.handlers.lock().unwrap_or_else(|e| e.into_inner());
        match pool.1.pop() {
            Some(handler) => handler,
            None => pool.0.clone(),
        }
    }

    fn return_handler(&self, handler: ReadRequestHandler) {
        let mut pool = self.handlers.lock().unwrap_or_else(|e| e.into_inner());
        if pool.1.len() < 1024 {
            pool.1.push(handler);
        }
    }

    pub fn declaration(&self) -> Option<&Declaration> {
        self.declaration.as_ref()
    }

    /// Production mode: start batch `sequence` (must follow the current checkpoint). Reads
    /// are excluded until the writer finishes; with `large`, they go upstream instead of
    /// waiting. Lock order is core, then gate, as everywhere else.
    pub async fn begin_batch(self: &Arc<Self>, sequence: u64, large: bool) -> Result<BatchWriter> {
        self.check_alive()?;
        if large {
            self.large_batch
                .store(true, std::sync::atomic::Ordering::Release);
        }
        let mut core = Arc::clone(&self.core).lock_owned().await;
        let gate = Arc::clone(&self.gate).write_owned().await;
        let checked = async {
            ensure!(!core.oracle, "oracle mode applies whole batches");
            let Core { sink, handle, .. } = &mut *core;
            let current = sink.checkpoint(handle).await?.sequence();
            ensure!(
                current.checked_add(1) == Some(sequence),
                "batch gap: checkpoint {current}, batch {sequence}"
            );
            Ok(())
        }
        .await;
        if let Err(error) = checked {
            // Nothing was applied; the replica is unchanged.
            self.large_batch
                .store(false, std::sync::atomic::Ordering::Release);
            return Err(error);
        }
        Ok(BatchWriter {
            engine: Arc::clone(self),
            core,
            _gate: gate,
            sequence,
            finished: false,
            inserts: 0,
            deletes: 0,
            cancelled: 0,
        })
    }

    pub async fn apply(&self, batch: &Batch) -> Result<Value> {
        self.check_alive()?;
        let mut core = self.core.lock().await;
        let _gate = self.gate.write().await;
        let result = core.apply(batch).await;
        self.guard(result)
    }

    pub async fn barrier(&self, rows: Vec<SourceRow>, missing: Vec<i64>) -> Result<Value> {
        self.check_alive()?;
        let mut core = self.core.lock().await;
        let _gate = self.gate.write().await;
        let result = core.barrier(rows, missing).await;
        self.guard(result)
    }

    /// Apply one batch and complete its fenced barrier with reads excluded throughout.
    /// Lock order everywhere is core, then gate.
    pub async fn apply_and_barrier(
        &self,
        batch: &Batch,
        rows: Vec<SourceRow>,
        missing: Vec<i64>,
    ) -> Result<(Value, Value)> {
        self.check_alive()?;
        let mut core = self.core.lock().await;
        let _gate = self.gate.write().await;
        let result = async {
            let applied = core.apply(batch).await?;
            ensure!(
                applied["ok"] == true && applied["checkpoint"] == batch.sequence,
                "engine apply not acknowledged: {applied}"
            );
            let barrier = core.barrier(rows, missing).await?;
            Ok((applied, barrier))
        }
        .await;
        self.guard(result)
    }

    pub async fn verify(
        &self,
        label: String,
        rows: Vec<SourceRow>,
        missing: Vec<i64>,
    ) -> Result<Value> {
        self.check_alive()?;
        let mut core = self.core.lock().await;
        let result = verify_reader(&mut core.reader, label, &rows, &missing).await;
        self.guard(result)
    }

    /// Native read through the in-process reader. Never takes the core lock, so concurrent
    /// reads only wait for a batch + barrier or a route removal.
    pub async fn query(&self, sql: &str, database: &str) -> Result<ReadOutcome> {
        self.check_alive()?;
        let Some(query) = self
            .declaration
            .as_ref()
            .and_then(|d| d.read_query(sql, database))
        else {
            return Ok(ReadOutcome::NotMatched);
        };
        // During a long batch, serve from upstream instead of waiting for the gate.
        let _gate = if self.large_batch.load(std::sync::atomic::Ordering::Acquire) {
            match self.gate.try_read() {
                Ok(gate) => gate,
                Err(_) => return Ok(ReadOutcome::NotMatched),
            }
        } else {
            self.gate.read().await
        };
        let routes = Arc::clone(&self.routes.read().unwrap_or_else(|e| e.into_inner()));
        let Some((route, params)) = routes.iter().find_map(|route| {
            route
                .template
                .query
                .read_params(&query)
                .map(|params| (route, params))
        }) else {
            return Ok(ReadOutcome::NotMatched);
        };
        let mut view = route.view.clone();
        let mut handler = self.take_handler();
        let result = native_query::read_with(&mut view, Some(&mut handler), &query, &params).await;
        self.return_handler(handler);
        Ok(match self.guard(result)? {
            Some((data, schema)) => ReadOutcome::Matched {
                template: route.template.name.clone(),
                schema,
                data,
            },
            None => ReadOutcome::LimitMiss,
        })
    }

    /// CREATE / DROP / SHOW CACHE. `Ok` with `rejected` for planning errors (no mutation);
    /// `Err` for any uncertain mutation, after which the engine is failed closed.
    pub async fn cache_ddl(&self, sql: &str, database: &str) -> Result<Value> {
        self.check_alive()?;
        let mut core = self.core.lock().await;
        let planned = self
            .declaration
            .as_ref()
            .context("source binding required")
            .and_then(|d| cache_control::plan(sql, database, d, &core.templates));
        let control = match planned {
            Err(error) => {
                return Ok(json!({"ok":false,"rejected":true,"error":format!("{error:#}")}));
            }
            Ok(control) => control,
        };
        let result = self.apply_control(&mut core, control).await;
        self.guard(result)
    }

    async fn apply_control(&self, core: &mut Core, control: CacheControl) -> Result<Value> {
        // After this point, an uncertain migration/RPC failure is fatal. Do not publish the
        // new route or retry a mutation with unknown outcome.
        ensure!(
            core.sink.checkpoint(&mut core.handle).await?.sequence() == core.barrier_checkpoint,
            "cache DDL requires a completed replication barrier"
        );
        match control {
            CacheControl::Create(template) => {
                core.sink
                    .wait_persisted(&mut core.handle, core.barrier_checkpoint)
                    .await?;
                let mut r = DeclaredReader {
                    view: create_reader(&mut core.handle, &template, &core.internal).await?,
                    witnessed: HashSet::new(),
                };
                let template = Arc::new(*template);
                if core.oracle {
                    fence_template(&template, &mut r, &core.barrier_rows).await?;
                } else {
                    // The initial replay of a big table can take minutes; existing routes keep
                    // serving meanwhile. Then a short marker fence, with reads excluded (they
                    // would briefly see the marker row), proves the reader is current.
                    let projection = Projection::Template(Arc::clone(&template));
                    wait_readable(&mut r.view, &projection).await?;
                    let _gate = self.gate.write().await;
                    let mut targets = fence_targets(core);
                    targets.push((r.view.clone(), projection));
                    marker_fence(&mut core.table, &mut targets).await?;
                }
                core.templates.push(template);
                core.declared.push(r);
                self.publish(core);
            }
            CacheControl::Drop(name) => {
                let i = core
                    .templates
                    .iter()
                    .position(|t| t.name == name)
                    .context("cache missing")?;
                {
                    // No read may hold the old route while its reader is removed.
                    let _gate = self.gate.write().await;
                    if !core.templates[i].query.same_shape(&core.internal) {
                        ensure!(
                            core.handle.remove_query(&name.into()).await? == 1,
                            "cache missing in controller"
                        );
                    }
                    core.templates.remove(i);
                    core.declared.remove(i);
                    self.publish(core);
                }
                if !core.oracle {
                    // Production mode: survivors are fenced by the next batch.
                    return Ok(cache_listing(&core.templates));
                }
                // A drop must not invalidate any surviving query or the shared base.
                verify_reader(
                    &mut core.reader,
                    "post-drop barrier".into(),
                    &core.barrier_rows,
                    &core.barrier_missing,
                )
                .await?;
                let Core {
                    templates,
                    declared,
                    barrier_rows,
                    ..
                } = core;
                for (t, r) in templates.iter().zip(declared.iter_mut()) {
                    fence_template(t, r, barrier_rows).await?;
                }
            }
            CacheControl::Show => {}
        }
        Ok(cache_listing(&core.templates))
    }

    /// Recreate the sink, controller client and table mutator (M2 recovery experiment).
    /// Durable engine metadata, not any in-memory counter, determines the next sequence.
    pub async fn reset_sink(&self) -> Result<Value> {
        self.check_alive()?;
        let mut core = self.core.lock().await;
        let result = async {
            core.sink = Sink::new(core.generation.clone(), LOCAL_TABLE.into())?;
            let mut raw = ReadySetHandle::make_raw(
                core.handle.get_address().clone(),
                Some(Duration::from_secs(30)),
                None,
            );
            let current = core.sink.checkpoint(&mut raw).await?;
            core.table = raw.table(LOCAL_TABLE).await?;
            Ok(json!({"ok":true,"reset_sink":true,"checkpoint":current.sequence()}))
        }
        .await;
        self.guard(result)
    }

    pub async fn checkpoint(&self) -> Result<u64> {
        let mut core = self.core.lock().await;
        let Core { sink, handle, .. } = &mut *core;
        Ok(sink.checkpoint(handle).await?.sequence())
    }

    pub async fn materializations(&self) -> Result<Value> {
        let mut core = self.core.lock().await;
        Ok(serde_json::to_value(
            core.handle.materialization_info().await?,
        )?)
    }

    /// Stop the in-process server. Must be called once, at process exit.
    pub async fn shutdown(&self) {
        let mut core = self.core.lock().await;
        if let Some(shutdown) = core.shutdown.take() {
            shutdown.shutdown_timeout(Duration::from_secs(30)).await;
        }
    }
}
