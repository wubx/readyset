//! Multi-source adapter over ONE native ReadySet server. Tables have separate read gates
//! and failure flags. The upstream position is each table's Databend Stream; a batch is
//! committed there only after all of its native Table RPCs ACKed and readers are fenced.
//! Restarts reuse ReadySet's own persistence (standalone authority recipe + base RocksDB,
//! synchronous WAL). A per-table restart marker in the native replication offset
//! ([`crate::restart_marker`]) tells whether the persisted rows match the Stream; only
//! tables that do not are rebuilt. Graph migrations remain exclusive. Storage, replay, and
//! reader fences are not reimplemented.
use crate::Action;
use crate::cache_control::{self, CacheControl};
use crate::declaration::Template;
use crate::engine::{CacheConfig, ReadOutcome, start_native_persistent};
use crate::native_query::{self, Input};
use crate::replicator::{Replicator, TypedChange};
use crate::restart_marker::{Decision, Marker, Phase, decide};
use crate::source_schema::{SourceCatalog, SourceSchema, SourceType};
use anyhow::{Context, Result, bail, ensure};
use readyset_client::recipe::changelist::{Change, ChangeList};
use readyset_client::{ReaderAddress, Table, TableOperation, View};
use readyset_data::{DfValue, Dialect};
use readyset_server::worker::readers::ReadRequestHandler;
use readyset_server::{Handle, Readers};
use readyset_sql::analysis::ReferredColumns;
use readyset_sql::ast::{Relation, SelectStatement, TableExprInner, TrxCachePolicy};
use readyset_util::shutdown::ShutdownSender;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::*};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::Duration;
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

struct Source {
    schema: Arc<SourceSchema>,
    /// Replaced only when the base table is dropped and recreated by a rebuild.
    table: Arc<Mutex<Table>>,
    gate: Arc<RwLock<()>>,
    failed: AtomicBool,
    busy: AtomicBool,
    /// Last sequence whose batch completed (all native ACKs + fence). Only advanced while
    /// holding `table`; it orders this source's batches, not a durability claim.
    checkpoint: AtomicU64,
    /// Whether the persisted native recipe already had this base table at startup.
    existed: bool,
    /// Last restart marker written to (or recovered from) this table's native offset.
    /// Only changed while holding `table`, in the same ordered channel as row writes.
    marker: StdMutex<Option<Marker>>,
    digest: String,
}
impl Source {
    fn marker(&self) -> Option<Marker> {
        self.marker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    /// Store the next marker through the table's own synchronous native channel.
    async fn write_marker(&self, table: &mut Table, phase: Phase) -> Result<()> {
        let next = match self.marker() {
            Some(m) if m.digest == self.digest => m.next(phase),
            Some(m) => Marker {
                digest: self.digest.clone(),
                phase,
                counter: m.counter + 1,
            },
            None => Marker {
                digest: self.digest.clone(),
                phase,
                counter: 1,
            },
        };
        table.set_replication_offset(next.offset()?).await?;
        *self.marker.lock().unwrap_or_else(|e| e.into_inner()) = Some(next);
        Ok(())
    }
}

/// A user cache declaration. Native ReadySet persists the cache itself in its recipe; this
/// adapter-side list only keeps the original SQL needed to rebuild the read route.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Declaration {
    name: String,
    database: String,
    sql: String,
}
/// Dropping an HTTP request future is not evidence that its graph RPC was cancelled.
struct MigrationGuard<'a> {
    failed: &'a AtomicBool,
    complete: bool,
}
impl Drop for MigrationGuard<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.failed.store(true, Release);
        }
    }
}

struct Route {
    source: String,
    template: Arc<Template>,
    view: View,
}

pub struct SharedEngine {
    handle: Mutex<Handle>,
    shutdown: StdMutex<Option<ShutdownSender>>,
    handler: StdMutex<ReadRequestHandler>,
    readers: Readers,
    catalog: SourceCatalog,
    sources: BTreeMap<String, Arc<Source>>,
    routes: StdRwLock<Vec<Arc<Route>>>,
    /// Batches hold a shared guard for their complete lifetime; only graph changes/snapshots
    /// take the exclusive side. Per-source Table ownership orders writes, not this lock.
    migrations: Arc<RwLock<()>>,
    cache: CacheConfig,
    /// `caches.json` next to the native storage; rewritten atomically.
    declarations_path: PathBuf,
    declarations: StdMutex<Vec<Declaration>>,
    failed: AtomicBool,
    hits: AtomicU64,
    misses: AtomicU64,
    timeouts: AtomicU64,
}

pub fn source_id(schema: &SourceSchema) -> String {
    format!("{}.{}", schema.database, schema.table)
}

impl SharedEngine {
    /// Start (or reopen) the ONE native server in `storage`. ReadySet's standalone authority
    /// restores the recipe; existing base tables reopen with their rows and restart
    /// markers. Only missing base tables are created here. Callers then decide per table
    /// ([`Self::decide`]) between [`Self::resume`] and [`Self::snapshot`].
    pub async fn start(
        storage: &Path,
        deployment: &str,
        cache: CacheConfig,
        schemas: Vec<Arc<SourceSchema>>,
    ) -> Result<Arc<Self>> {
        // Synchronous native WAL: an ACKed Table RPC (rows or marker) survives a crash and
        // an in-process domain recovery.
        let mut native = start_native_persistent(storage, deployment, cache).await?;
        let mut controller = native
            .handle
            .c
            .clone()
            .context("native controller missing")?;
        let existing = controller.tables().await?;
        let offsets = if existing.is_empty() {
            Default::default()
        } else {
            controller.replication_offsets().await?.tables
        };
        let local = |schema: &SourceSchema| {
            existing
                .keys()
                .find(|r| r.name.as_str() == schema.local)
                .cloned()
        };
        let missing: Vec<_> = schemas
            .iter()
            .filter(|s| local(s).is_none())
            .map(|s| s.ddl())
            .collect();
        if !missing.is_empty() {
            native
                .handle
                .extend_recipe(ChangeList::from_strings(missing, Dialect::DEFAULT_MYSQL)?)
                .await?;
        }
        let mut catalog = SourceCatalog::default();
        let mut sources = BTreeMap::new();
        for schema in schemas {
            catalog.insert((*schema).clone())?;
            let relation = local(&schema);
            // An unparsable marker is treated like a missing one: the table is rebuilt.
            let marker = relation
                .as_ref()
                .and_then(|r| offsets.get(r).cloned().flatten())
                .and_then(|o| Marker::parse(&o).ok());
            let table = native.handle.table(schema.local.as_str()).await?;
            let digest = hex::encode(Sha256::digest(schema.ddl().as_bytes()))[..12].to_string();
            sources.insert(
                source_id(&schema),
                Arc::new(Source {
                    schema,
                    table: Arc::new(Mutex::new(table)),
                    gate: Arc::new(RwLock::new(())),
                    failed: AtomicBool::new(false),
                    busy: AtomicBool::new(false),
                    checkpoint: AtomicU64::new(0),
                    existed: relation.is_some(),
                    marker: StdMutex::new(marker),
                    digest,
                }),
            );
        }
        let declarations_path = storage.join("caches.json");
        let declarations: Vec<Declaration> = match std::fs::read(&declarations_path) {
            Ok(bytes) => serde_json::from_slice(&bytes).context("caches.json")?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e).context("caches.json"),
        };
        // A native cache without a declaration was created without a durable acknowledgement
        // (crash between the two steps) or was dropped by the user: remove it.
        for name in controller.views().await?.into_keys() {
            if !declarations.iter().any(|d| d.name == name.name.as_str()) {
                native.handle.remove_query(&name).await?;
            }
        }
        Ok(Arc::new(Self {
            handle: Mutex::new(native.handle),
            shutdown: StdMutex::new(Some(native.shutdown)),
            handler: StdMutex::new(native.handler),
            readers: native.readers,
            catalog,
            sources,
            routes: StdRwLock::new(Vec::new()),
            migrations: Arc::new(RwLock::new(())),
            cache,
            declarations_path,
            declarations: StdMutex::new(declarations),
            failed: AtomicBool::new(false),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
        }))
    }
    /// Restart decision for one configured source from its recovered native marker.
    pub fn decide(&self, id: &str, stream_version: u64) -> Result<Decision> {
        let source = self.sources.get(id).context("source missing")?;
        Ok(decide(
            source.existed,
            source.marker().as_ref(),
            &source.digest,
            stream_version,
        ))
    }
    /// Continue from the persisted rows: record `Applied(version)` if the last marker was an
    /// interrupted-but-committed batch, then rebind this source's declared caches.
    pub async fn resume(&self, id: &str, version: u64) -> Result<()> {
        self.alive()?;
        let _migration = self.migrations.write().await;
        let source = self.sources.get(id).context("source missing")?;
        let mut table = source.table.lock().await;
        if source.marker().map(|m| m.phase) != Some(Phase::Applied(version)) {
            source
                .write_marker(&mut table, Phase::Applied(version))
                .await?;
        }
        self.restore_caches(id, source, &mut table).await?;
        source.checkpoint.store(1, Release);
        Ok(())
    }
    /// The Databend COMMIT of this source's batch succeeded at `version`.
    pub async fn committed(&self, id: &str, version: u64) -> Result<()> {
        let source = self.sources.get(id).context("source missing")?;
        let mut table = source.table.lock().await;
        if source.marker().map(|m| m.phase) != Some(Phase::Applied(version)) {
            source
                .write_marker(&mut table, Phase::Applied(version))
                .await?;
        }
        Ok(())
    }
    pub fn marker_phase(&self, id: &str) -> Option<Phase> {
        self.sources.get(id)?.marker().map(|m| m.phase)
    }
    fn alive(&self) -> Result<()> {
        ensure!(
            !self.failed.load(Acquire),
            "shared native engine failed closed"
        );
        Ok(())
    }
    pub fn poison(&self) {
        self.failed.store(true, Release);
    }
    pub fn is_failed(&self) -> bool {
        self.failed.load(Acquire)
    }
    pub fn source_busy(&self, id: &str) -> bool {
        self.sources
            .get(id)
            .is_some_and(|s| s.busy.load(Acquire) || s.gate.try_read().is_err())
    }
    pub fn fail_source(&self, id: &str) {
        if let Some(source) = self.sources.get(id) {
            source.failed.store(true, Release);
        }
    }
    fn bind(&self, select: &SelectStatement, database: &str) -> Result<Arc<SourceSchema>> {
        ensure!(
            select.tables.len() == 1 && select.join.is_empty(),
            "one configured base table required; JOIN stays upstream"
        );
        let TableExprInner::Table(relation) = &select.tables[0].inner else {
            bail!("base table required")
        };
        self.catalog
            .resolve(relation, database)
            .cloned()
            .context("source is not configured or its schema is unsupported")
    }
    pub fn read_source(&self, sql: &str, database: &str) -> Option<String> {
        let select = native_query::parse_read_select(sql).ok()?;
        let schema = self.bind(&select, database).ok()?;
        native_query::compile_for_source(select, &schema, database, Input::Read).ok()?;
        Some(source_id(&schema))
    }
    fn routes_for(&self, id: &str) -> Vec<View> {
        self.routes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|r| r.source == id)
            .map(|r| r.view.clone())
            .collect()
    }
    /// Load a fresh snapshot. `rebuild` first drops the stale native base table (ReadySet
    /// deletes its RocksDB directory) and its caches, then recreates it from the current
    /// schema. A `Rebuilding` marker precedes every row, so a crash mid-load rebuilds again.
    /// Declared caches of this source are recreated afterwards and fenced.
    pub async fn snapshot(&self, id: &str, capture: &mut Replicator, rebuild: bool) -> Result<u64> {
        self.alive()?;
        let _migration = self.migrations.write().await;
        self.alive()?;
        let source = self.sources.get(id).context("source missing")?;
        let mut table = source.table.lock().await;
        if rebuild {
            let mut handle = self.handle.lock().await;
            let mut controller = handle.c.clone().context("native controller missing")?;
            let views = controller.views().await?;
            for d in self.declarations_for(id) {
                if let Some(name) = views.keys().find(|v| v.name.as_str() == d.name) {
                    handle.remove_query(name).await?;
                }
            }
            let local: Relation = source.schema.local.as_str().into();
            handle
                .extend_recipe(ChangeList::from_change(
                    Change::Drop {
                        name: local,
                        if_exists: true,
                    },
                    Dialect::DEFAULT_MYSQL,
                ))
                .await?;
            handle
                .extend_recipe(ChangeList::from_strings(
                    vec![source.schema.ddl()],
                    Dialect::DEFAULT_MYSQL,
                )?)
                .await?;
            *table = handle.table(source.schema.local.as_str()).await?;
        }
        source.write_marker(&mut table, Phase::Rebuilding).await?;
        let rows = capture.snapshot_into(&mut table).await?;
        capture.check_schema().await?;
        self.restore_caches(id, source, &mut table).await?;
        source.checkpoint.store(0, Release);
        Ok(rows)
    }
    fn declarations_for(&self, id: &str) -> Vec<Declaration> {
        self.declarations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|d| {
                native_query::parse_select(&d.sql)
                    .ok()
                    .and_then(|s| self.bind(&s, &d.database).ok())
                    .is_some_and(|s| source_id(&s) == id)
            })
            .cloned()
            .collect()
    }
    /// Rebind (or recreate) this source's declared caches. Caller holds the exclusive
    /// migration guard and the source's Table. A declaration that no longer compiles for
    /// the current schema is removed from the native recipe and from `caches.json`.
    async fn restore_caches(&self, id: &str, source: &Source, table: &mut Table) -> Result<()> {
        let mut controller = self
            .handle
            .lock()
            .await
            .c
            .clone()
            .context("native controller missing")?;
        let views = controller.views().await?;
        let mut created = Vec::new();
        let mut stale = Vec::new();
        for d in self.declarations_for(id) {
            let native = views.keys().find(|v| v.name.as_str() == d.name).cloned();
            let template = match self.template(d.name.clone(), &d.sql, &d.database) {
                Ok(t) => t,
                Err(_) => {
                    if let Some(name) = native {
                        self.handle.lock().await.remove_query(&name).await?;
                    }
                    stale.push(d.name);
                    continue;
                }
            };
            let mut handle = self.handle.lock().await;
            if native.is_none() {
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
            }
            let view = handle.view(template.name.as_str()).await?;
            drop(handle);
            created.push(Arc::new(Route {
                source: id.into(),
                template: Arc::new(template),
                view,
            }));
        }
        if !created.is_empty() {
            let mut views: Vec<View> = created.iter().map(|r| r.view.clone()).collect();
            self.fence(source, table, &mut views).await?;
            self.routes
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .extend(created);
        }
        if !stale.is_empty() {
            self.save_declarations(|all| all.retain(|d| !stale.contains(&d.name)))?;
        }
        Ok(())
    }
    /// Update and atomically persist `caches.json` (temp file, fsync, rename, fsync dir).
    fn save_declarations(&self, update: impl FnOnce(&mut Vec<Declaration>)) -> Result<()> {
        let mut all = self.declarations.lock().unwrap_or_else(|e| e.into_inner());
        let mut next = all.clone();
        update(&mut next);
        let tmp = self.declarations_path.with_extension("json.tmp");
        {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&serde_json::to_vec_pretty(&next)?)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &self.declarations_path)?;
        if let Some(dir) = self.declarations_path.parent() {
            std::fs::File::open(dir)?.sync_all()?;
        }
        *all = next;
        Ok(())
    }
    pub async fn begin_batch(self: &Arc<Self>, id: &str, sequence: u64) -> Result<SharedBatch> {
        self.alive()?;
        let migration = Arc::clone(&self.migrations).read_owned().await;
        self.alive()?;
        let source = Arc::clone(self.sources.get(id).context("source missing")?);
        // Lock BEFORE checking sequence/failed: two same-source begin calls must not both
        // validate an old sequence and subsequently apply the same batch twice.
        let table = Arc::clone(&source.table).lock_owned().await;
        self.alive()?;
        ensure!(!source.failed.load(Acquire), "source failed closed");
        ensure!(
            source.checkpoint.load(Acquire).checked_add(1) == Some(sequence),
            "per-source sequence gap or uncompleted batch"
        );
        Ok(SharedBatch {
            engine: Arc::clone(self),
            source,
            id: id.into(),
            table,
            _gate: None,
            _migration: migration,
            sequence,
            done: false,
            marked: false,
            inserts: 0,
            deletes: 0,
            cancelled: 0,
        })
    }
    pub async fn query(&self, sql: &str, database: &str) -> Result<ReadOutcome> {
        self.alive()?;
        let Some(id) = self.read_source(sql, database) else {
            return Ok(ReadOutcome::NotMatched);
        };
        let source = &self.sources[&id];
        if source.failed.load(Acquire) || source.busy.load(Acquire) {
            return Ok(ReadOutcome::NotMatched);
        }
        let Ok(_gate) = source.gate.try_read() else {
            return Ok(ReadOutcome::NotMatched);
        };
        // Recheck after acquiring the gate: a failed writer may have released it meanwhile.
        if source.failed.load(Acquire) {
            return Ok(ReadOutcome::NotMatched);
        }
        let query = native_query::compile_for_source(
            native_query::parse_read_select(sql)?,
            &source.schema,
            database,
            Input::Read,
        )?;
        let routes = self
            .routes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let Some((route, params)) = routes
            .iter()
            .filter(|r| r.source == id)
            .find_map(|r| r.template.query.read_params(&query).map(|p| (r, p)))
        else {
            return Ok(ReadOutcome::NotMatched);
        };
        let mut view = route.view.clone();
        let misses = if self.cache.partial {
            match view.build_view_query(
                params.make_keys::<DfValue>(&[])?,
                None,
                None,
                Dialect::DEFAULT_MYSQL,
                None,
                None,
            )? {
                Some((reader, query)) => {
                    let address = ReaderAddress {
                        node: *reader.node(),
                        name: reader.name().clone(),
                    };
                    let readers = self.readers.lock().unwrap_or_else(|e| e.into_inner());
                    let local = readers.get(&address).context("native reader missing")?;
                    query
                        .key_comparisons
                        .iter()
                        .filter(|k| {
                            !k.is_reversed_range() && !matches!(local.contains(k), Ok(true))
                        })
                        .count() as u64
                }
                None => 0,
            }
        } else {
            0
        };
        let mut handler = self
            .handler
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let result = native_query::read_with(&mut view, Some(&mut handler), &query, &params).await;
        if result.as_ref().err().is_some_and(|e| {
            e.downcast_ref::<readyset_errors::ReadySetError>()
                .is_some_and(|e| {
                    e.any_cause(|e| matches!(e, readyset_errors::ReadySetError::UpqueryTimeout))
                })
        }) {
            self.timeouts.fetch_add(1, Relaxed);
            return Ok(ReadOutcome::NotMatched);
        }
        let result = result?;
        if misses > 0 {
            self.misses.fetch_add(misses, Relaxed);
        } else {
            self.hits.fetch_add(1, Relaxed);
        }
        Ok(match result {
            Some((data, schema)) => ReadOutcome::Matched {
                template: route.template.name.clone(),
                schema,
                data,
            },
            None => ReadOutcome::LimitMiss,
        })
    }
    /// Compile one cache declaration for its configured source (used by CREATE CACHE and
    /// when rebinding persisted declarations after a restart).
    fn template(&self, name: String, sql: &str, database: &str) -> Result<Template> {
        let select = native_query::parse_select(sql)?;
        let source = self.bind(&select, database)?;
        let query =
            native_query::compile_for_source(select, &source, database, Input::Declaration)?;
        let physical_columns = query.projection.iter().map(String::as_str).chain(
            query
                .statement
                .where_clause
                .iter()
                .flat_map(|e| e.referred_columns())
                .map(|c| c.name.as_str()),
        );
        ensure!(
            physical_columns
                .into_iter()
                .any(|name| source.columns.iter().any(|c| c.name == name
                    && match c.ty {
                        SourceType::String => true,
                        SourceType::Signed(bits) | SourceType::Unsigned(bits) => bits == 64,
                        SourceType::Decimal { precision, .. } => precision >= 20,
                        _ => false,
                    })),
            "cache physical projection has only small-domain columns; cannot prove a collision-free marker fence"
        );
        Ok(Template {
            name,
            source_sql: sql.into(),
            query,
        })
    }
    pub async fn cache_ddl(
        &self,
        sql: &str,
        database: &str,
        available: impl Fn(&str) -> bool + Send,
    ) -> Result<Value> {
        self.alive()?;
        let _migration = self.migrations.write().await;
        self.alive()?;
        let routes = self
            .routes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let templates: Vec<_> = routes.iter().map(|r| Arc::clone(&r.template)).collect();
        let planned = cache_control::plan_with(sql, &templates, |name, sql| {
            let select = native_query::parse_select(sql)?;
            let schema = self.bind(&select, database)?;
            let id = source_id(&schema);
            ensure!(
                available(&id) && !self.sources[&id].failed.load(Acquire),
                "source is not ready"
            );
            self.template(name, sql, database)
        });
        let control = match planned {
            Ok(c) => c,
            Err(e) => return Ok(json!({"ok":false,"rejected":true,"error":format!("{e:#}")})),
        };
        let mut migration = MigrationGuard {
            failed: &self.failed,
            complete: matches!(&control, CacheControl::Show),
        };
        let result = async {
            match control {
                CacheControl::Create(template) => {
                    let select = native_query::parse_select(&template.source_sql)?;
                    let schema = self.bind(&select, database)?;
                    let id = source_id(&schema);
                    let source = &self.sources[&id];
                    let mut handle = self.handle.lock().await;
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
                    let view = handle.view(template.name.as_str()).await?;
                    drop(handle);
                    let _gate = source.gate.write().await;
                    let mut table = source.table.lock().await;
                    let mut targets = self.routes_for(&id);
                    targets.push(view.clone());
                    self.fence(source, &mut table, &mut targets).await?;
                    // Durable acknowledgement of the declaration. A crash before this line
                    // leaves an undeclared native cache, which the next start removes.
                    let declaration = Declaration {
                        name: template.name.clone(),
                        database: database.into(),
                        sql: template.source_sql.clone(),
                    };
                    self.save_declarations(|all| {
                        all.retain(|d| d.name != declaration.name);
                        all.push(declaration);
                    })?;
                    self.routes
                        .write()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(Arc::new(Route {
                            source: id,
                            template: Arc::new(*template),
                            view,
                        }));
                }
                CacheControl::Drop(name) => {
                    let route = routes
                        .iter()
                        .find(|r| r.template.name == name)
                        .context("cache missing")?;
                    let _gate = self.sources[&route.source].gate.write().await;
                    // Forget the declaration first: a crash after this leaves an undeclared
                    // native cache that the next start removes, never a dangling declaration.
                    self.save_declarations(|all| all.retain(|d| d.name != name))?;
                    ensure!(
                        self.handle
                            .lock()
                            .await
                            .remove_query(&name.as_str().into())
                            .await?
                            == 1,
                        "native cache missing"
                    );
                    self.routes
                        .write()
                        .unwrap_or_else(|e| e.into_inner())
                        .retain(|r| r.template.name != name);
                }
                CacheControl::Show => {}
            }
            Ok::<_, anyhow::Error>(self.listing())
        }
        .await;
        migration.complete = result.is_ok();
        result
    }
    fn listing(&self) -> Value {
        let routes = self.routes.read().unwrap_or_else(|e| e.into_inner());
        json!({"ok":true,"schema":[{"name":"name","type":"String"},{"name":"query","type":"String"},{"name":"state","type":"String"}],
            "data":routes.iter().map(|r| vec![r.template.name.clone(),r.template.source_sql.clone(),if self.sources[&r.source].failed.load(Acquire) {"error".into()} else {"ready".into()}]).collect::<Vec<_>>(),
            "caches":routes.iter().map(|r| &r.template.name).collect::<Vec<_>>()})
    }
    async fn fence(&self, source: &Source, table: &mut Table, targets: &mut [View]) -> Result<()> {
        let columns: Vec<_> = source
            .schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        crate::engine_fence::fence(
            table,
            targets,
            &columns,
            &self.readers,
            || marker(&source.schema),
            Duration::from_secs(1800),
        )
        .await
    }
    /// Fault-injection discovery only (`--enable-fault-injection`): the native HTTP router,
    /// which serves ReadySet's own `/failpoint` in `failure_injection` builds, and the
    /// native domain index owning each configured base table. Read-only controller RPCs.
    pub async fn native_fault_targets(&self) -> Result<Value> {
        let mut controller = self
            .handle
            .lock()
            .await
            .c
            .as_ref()
            .context("native controller missing")?
            .clone();
        let uri = controller.controller_uri().await?;
        let tables = controller.tables().await?;
        let info = controller.get_info().await?;
        let mut domains = BTreeMap::new();
        for (id, source) in &self.sources {
            let node = tables
                .iter()
                .find(|(name, _)| name.name.as_str() == source.schema.local)
                .map(|(_, node)| *node)
                .context("native base table missing")?;
            let domain = info
                .values()
                .flat_map(|domains| domains.iter())
                .find(|(_, nodes)| nodes.contains(&node))
                .map(|(domain, _)| domain.index())
                .context("native base domain missing")?;
            domains.insert(id.clone(), domain);
        }
        Ok(json!({"native_http": uri.to_string(), "base_domains": domains}))
    }
    pub fn cache_status(&self) -> Value {
        let readers: Vec<_> = self.readers.lock().unwrap_or_else(|e| e.into_inner()).iter_mut().map(|(a,r)| json!({"name":a.name.name.as_str(),"key_slots":r.len(),"eviction_epoch":r.eviction_epoch()})).collect();
        json!({"failed":self.is_failed(),"native_server_instances":1,"base_tables":self.sources.len(),"durability":"native_sync_wal","materialization":if self.cache.partial {"partial"} else {"full"},
            "memory_limit_bytes":self.cache.memory_limit_bytes,"upquery_timeout_ms":self.cache.upquery_timeout.as_millis(),
            "allocated_bytes":readyset_alloc::fetch_stats().ok().map(|s| s.allocated),"readers":readers,
            "hit_queries":self.hits.load(Relaxed),"miss_keys":self.misses.load(Relaxed),"replay_timeouts":self.timeouts.load(Relaxed)})
    }
    pub async fn shutdown(&self) {
        let shutdown = self
            .shutdown
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(shutdown) = shutdown {
            shutdown.shutdown_timeout(Duration::from_secs(30)).await;
        }
    }
}

pub struct SharedBatch {
    engine: Arc<SharedEngine>,
    source: Arc<Source>,
    id: String,
    table: OwnedMutexGuard<Table>,
    _gate: Option<OwnedRwLockWriteGuard<()>>,
    _migration: OwnedRwLockReadGuard<()>,
    sequence: u64,
    done: bool,
    /// The `Applying` restart marker for this batch has been written.
    marked: bool,
    pub inserts: u64,
    pub deletes: u64,
    pub cancelled: u64,
}
impl SharedBatch {
    /// Record `Applying(last applied version)` before the first row of a batch that will be
    /// committed. Ordered before the rows in the same native channel. While a snapshot load
    /// is still `Rebuilding`, the marker stays as is: a crash then rebuilds anyway.
    pub async fn mark_applying(&mut self) -> Result<()> {
        if self.marked {
            return Ok(());
        }
        if let Some(Phase::Applied(version)) = self.source.marker().map(|m| m.phase) {
            self.source
                .write_marker(&mut self.table, Phase::Applying(version))
                .await?;
        }
        self.marked = true;
        Ok(())
    }
    pub async fn push(&mut self, changes: Vec<TypedChange>) -> Result<()> {
        let mut deleted: HashMap<Vec<DfValue>, usize> = HashMap::new();
        for c in changes.iter().filter(|c| c.action == Action::Delete) {
            *deleted.entry(c.row.clone()).or_default() += 1;
        }
        let mut inserted = Vec::new();
        for c in changes.into_iter().filter(|c| c.action == Action::Insert) {
            match deleted.get_mut(&c.row) {
                Some(n) if *n > 0 => {
                    *n -= 1;
                    self.cancelled += 1;
                }
                _ => inserted.push(c.row),
            }
        }
        let mut operations = Vec::new();
        for (row, n) in deleted {
            for _ in 0..n {
                operations.push(TableOperation::DeleteRow { row: row.clone() });
            }
        }
        self.deletes += operations.len() as u64;
        self.inserts += inserted.len() as u64;
        operations.extend(inserted.into_iter().map(TableOperation::Insert));
        if !operations.is_empty() && self._gate.is_none() {
            self.mark_applying().await?;
            self.source.busy.store(true, Release);
            self._gate = Some(Arc::clone(&self.source.gate).write_owned().await);
        }
        for chunk in operations.chunks(10000) {
            self.table.perform_all(chunk.to_vec()).await?;
        }
        Ok(())
    }
    pub async fn finish(mut self) -> Result<Value> {
        // Every chunk's Table RPC has ACKed in `push`. Fence readers only when rows changed;
        // the Databend COMMIT that follows is the sole position record.
        if self.inserts + self.deletes > 0 {
            let mut views = self.engine.routes_for(&self.id);
            self.engine
                .fence(&self.source, &mut self.table, &mut views)
                .await?;
        }
        self.source.checkpoint.store(self.sequence, Release);
        self.done = true;
        Ok(
            json!({"ok":true,"checkpoint":self.sequence,"inserts":self.inserts,"deletes":self.deletes,"cancelled":self.cancelled}),
        )
    }
}
impl Drop for SharedBatch {
    fn drop(&mut self) {
        if !self.done {
            self.source.failed.store(true, Release);
        }
        self.source.busy.store(false, Release);
    }
}

#[cfg(test)]
#[path = "shared_engine_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "shared_engine_concurrency_tests.rs"]
mod concurrency_tests;

#[cfg(test)]
#[path = "shared_engine_restart_tests.rs"]
mod restart_tests;

fn marker(schema: &SourceSchema) -> Result<Vec<DfValue>> {
    let nonce = uuid::Uuid::new_v4();
    let bits = nonce.as_u128();
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let n = (bits.rotate_right((i as u32 * 11) % 128)) as u64;
            let cell = match c.ty {
                SourceType::Boolean => json!((n & 1).to_string()),
                SourceType::String => json!(format!("rs-fence-{nonce}-{i}")),
                SourceType::Signed(width) => json!(((n as i64) >> (64 - width)).to_string()),
                SourceType::Unsigned(width) => json!((n >> (64 - width)).to_string()),
                SourceType::Decimal { precision, scale } => {
                    let unscaled =
                        bits.rotate_right((i as u32 * 11) % 128) % 10u128.pow(precision.into());
                    let digits = format!("{:0width$}", unscaled, width = usize::from(scale) + 1);
                    if scale == 0 {
                        json!(digits)
                    } else {
                        let split = digits.len() - usize::from(scale);
                        json!(format!("{}.{}", &digits[..split], &digits[split..]))
                    }
                }
            };
            c.decode(&cell)
        })
        .collect()
}
