//! Databend Stream change capture for one fixed-schema table.
//!
//! The only Databend object is one Stream per synchronized table, next to the table. Each
//! round runs `BEGIN; SELECT ... FROM stream WITH CONSUME; <apply to ReadySet>; COMMIT`:
//! the Stream only advances when the transaction commits, so a failed apply leaves the
//! changes in the Stream. Tables, cache declarations and progress live in ReadySet.
//!
//! Bootstrap keeps one native transaction across snapshot AT STREAM and its first consume:
//! repeatable-read pins Stream/source versions and COMMIT's native version CAS rejects a
//! previous consumer's late commit. Aborting that transaction requires a rebuild.
//!
//! Production mode streams the snapshot and every batch page by page and keeps no copy of
//! the table. Oracle mode (POC regression verifiers) also keeps every row to re-verify the
//! replica after each batch, and is bounded to small tables with unique ids.
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use readyset_client::Table;

use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::engine::Loader;
use crate::source_schema::SourceSchema;
use crate::upstream::{DatabendClient, Pages};
use crate::{Action, Batch, Change, SourceRow};

/// Oracle mode bounds (the engine re-verifies every row per batch).
pub const MAX_ROWS: usize = 9000;
pub const MAX_KNOWN_KEYS: usize = 12000;
const MAX_BATCH: usize = 10000;
/// Rows per HTTP page for snapshots and production batches (about 9 MB of JSON).
pub const PAGE_ROWS: usize = 50000;
/// Retries of one read-only admin metadata statement after a transient upstream failure
/// (backoff 0.4s, 0.8s, 1.6s).
const ADMIN_READ_RETRIES: u32 = 3;

/// An upstream failure that says nothing about the statement itself, so a read-only one
/// may simply be repeated: transport errors (connect, reset, timeout), HTTP 5xx / 429, and
/// Databend's internal connection error (code 2001, e.g. query node <-> meta broken pipe).
/// SQL errors (bad statement, missing object, permission) are never transient.
pub fn transient_upstream(error: &anyhow::Error) -> bool {
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<reqwest::Error>().is_some())
    {
        return true;
    }
    let text = format!("{error:#}");
    text.contains("Databend HTTP 5")
        || text.contains("Databend HTTP 429")
        || text.contains("\"code\":2001")
}

#[derive(Clone, Debug)]
pub struct Names {
    pub source: String,
    pub generation: String,
    pub stream: String,
}

impl Names {
    /// One Stream per instance and table, in the table's own database. Instances never
    /// share (and so never consume each other's) Streams.
    pub fn new(database: &str, table: &str, instance_id: &str, generation: &str) -> Self {
        Self {
            source: format!("{database}.{table}"),
            generation: generation.to_owned(),
            stream: format!("{database}.readyset_{instance_id}_{table}"),
        }
    }
}

fn text(cell: &Value, what: &str) -> Result<String> {
    cell.as_str()
        .map(str::to_owned)
        .with_context(|| format!("{what} is not a text value"))
}

fn int(cell: &Value, what: &str) -> Result<i64> {
    // HTTP fields are string encoded; parse directly, never through floating point.
    match cell {
        Value::String(s) => s.parse().with_context(|| format!("{what} is not a BIGINT")),
        Value::Number(n) => n
            .as_i64()
            .with_context(|| format!("{what} is not a BIGINT")),
        _ => bail!("NULL or invalid {what}"),
    }
}

pub fn parse_row(cells: &[Value]) -> Result<SourceRow> {
    ensure!(cells.len() == 4, "expected 4 columns, got {}", cells.len());
    Ok(SourceRow {
        id: int(&cells[0], "id")?,
        payload: text(&cells[1], "payload")?,
        score: int(&cells[2], "score")?,
        note: match &cells[3] {
            Value::Null => None,
            other => Some(text(other, "note")?),
        },
    })
}

pub fn parse_change(cells: &[Value]) -> Result<Change> {
    ensure!(cells.len() == 7, "invalid source change");
    let action = match cells[4].as_str() {
        Some("INSERT") => Action::Insert,
        Some("DELETE") => Action::Delete,
        _ => bail!("invalid change$action"),
    };
    let is_update = match &cells[6] {
        Value::Bool(b) => *b,
        Value::String(s) if s == "1" || s == "true" => true,
        Value::String(s) if s == "0" || s == "false" => false,
        _ => bail!("invalid change$is_update"),
    };
    Ok(Change {
        action,
        row_id: text(&cells[5], "change$row_id")?,
        is_update,
        row: parse_row(&cells[..4])?,
    })
}

pub fn parse_changes(page: &[Vec<Value>]) -> Result<Vec<Change>> {
    page.iter().map(|cells| parse_change(cells)).collect()
}

/// Full typed before/after images; no business-key uniqueness or primary-key inference.
pub struct TypedChange {
    pub action: Action,
    pub row: Vec<readyset_data::DfValue>,
}

pub fn parse_typed_changes(schema: &SourceSchema, page: &[Vec<Value>]) -> Result<Vec<TypedChange>> {
    let width = schema.columns.len();
    page.iter()
        .map(|cells| {
            ensure!(cells.len() == width + 3, "source change arity mismatch");
            let action = match cells[width].as_str() {
                Some("INSERT") => Action::Insert,
                Some("DELETE") => Action::Delete,
                _ => bail!("invalid change$action"),
            };
            ensure!(
                cells[width + 1].as_str().is_some_and(|s| !s.is_empty()),
                "missing change$row_id"
            );
            let update_valid = match &cells[width + 2] {
                Value::Bool(_) => true,
                Value::String(s) => matches!(s.as_str(), "true" | "false" | "0" | "1"),
                _ => false,
            };
            ensure!(update_valid, "invalid change$is_update");
            Ok(TypedChange {
                action,
                row: schema.decode_row(&cells[..width])?,
            })
        })
        .collect()
}

pub fn checksum(changes: &[Change]) -> Result<String> {
    // Same canonical encoding Batch::validate recomputes in the engine.
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(changes)?)))
}

/// Changes read inside an open transaction; the Stream advances only on `commit_capture`.
pub struct Prepared {
    pub batch: Batch,
    pub new_state: BTreeMap<i64, SourceRow>,
}

pub struct Replicator {
    pub names: Names,
    admin: DatabendClient,
    capture: DatabendClient,
    oracle: bool,
    /// Oracle mode only.
    pub state: BTreeMap<i64, SourceRow>,
    /// Oracle mode only.
    pub known: HashSet<i64>,
    pub sequence: u64,
    /// Current source row count (snapshot plus committed inserts minus deletes).
    pub rows: u64,
    pub default_role: Option<String>,
    /// Dynamic production binding. Oracle continues to use the original bounded DTO.
    pub schema: Option<Arc<SourceSchema>>,
    /// Snapshot and the first consuming read MUST share the native transaction's pinned
    /// Stream/source versions. Also bounds freshness: a long snapshot is not a fresh round.
    bootstrap_started: Option<Instant>,
    capture_failed: bool,
    /// Last successful Stream COMMIT (also set at resume/snapshot start for pacing).
    committed_at: Instant,
}

const EXPECTED_SCHEMA: [(&str, &str, &str); 4] = [
    ("id", "BIGINT", "NO"),
    ("payload", "VARCHAR", "NO"),
    ("score", "BIGINT", "NO"),
    ("note", "VARCHAR", "YES"),
];

impl Replicator {
    pub fn new(names: Names, admin: DatabendClient, capture: DatabendClient, oracle: bool) -> Self {
        Self {
            names,
            admin,
            capture,
            oracle,
            state: BTreeMap::new(),
            known: HashSet::new(),
            sequence: 0,
            rows: 0,
            default_role: None,
            schema: None,
            bootstrap_started: None,
            capture_failed: false,
            committed_at: Instant::now(),
        }
    }

    /// Time since the Stream was last advanced by this capture.
    pub fn since_commit(&self) -> std::time::Duration {
        self.committed_at.elapsed()
    }

    pub fn bootstrap_started(&self) -> Option<Instant> {
        self.bootstrap_started
    }

    /// The Stream's identity and offset: (`system.streams.stream_id`, `table_version` = the
    /// source table's meta seq). Only this instance consumes the Stream, so between our own
    /// COMMITs both are stable; a dropped and recreated Stream has a new id.
    pub async fn stream_version(&mut self) -> Result<(u64, u64)> {
        let (database, stream) = self
            .names
            .stream
            .split_once('.')
            .context("qualified stream name")?;
        let rows = self
            .admin_read(&format!(
                "SELECT stream_id, table_version, invalid_reason FROM system.streams WHERE database = '{database}' AND name = '{stream}'"
            ))
            .await?;
        ensure!(rows.len() == 1, "stream missing");
        let reason = rows[0][2].as_str().unwrap_or_default();
        ensure!(reason.is_empty(), "stream invalid: {reason}");
        let number = |v: &Value, what: &str| -> Result<u64> {
            match v {
                Value::String(s) => Ok(s.parse()?),
                Value::Number(n) => n.as_u64().with_context(|| what.to_string()),
                _ => bail!("{what} missing"),
            }
        };
        Ok((
            number(&rows[0][0], "stream id")?,
            number(&rows[0][1], "stream version")?,
        ))
    }

    /// Continue from persisted local rows that equal the source at the Stream offset: no
    /// snapshot. Later rounds BEGIN their own transactions as after a bootstrap commit.
    pub async fn resume(&mut self) -> Result<u64> {
        ensure!(
            self.sequence == 0 && self.bootstrap_started.is_none() && !self.capture_failed,
            "resume only before any capture"
        );
        let rows = self
            .admin
            .sql(&format!(
                "SELECT count(*) FROM {} AT (STREAM => {})",
                self.names.source, self.names.stream
            ))
            .await?;
        let rows: u64 = match rows.first().and_then(|r| r.first()) {
            Some(Value::String(s)) => s.parse()?,
            Some(Value::Number(n)) => n.as_u64().context("row count")?,
            _ => bail!("row count missing"),
        };
        self.sequence = 1;
        self.rows = rows;
        Ok(rows)
    }

    /// Advance the Stream to the source table's current snapshot, discarding its pending
    /// changes (native consuming read in its own transaction). Only for a table that is about
    /// to be rebuilt because its schema changed: a snapshot AT the old Stream offset predates
    /// the DDL and cannot produce the new columns. The rebuild then snapshots AT the new
    /// offset as usual. Stream version CAS still rejects a concurrent consumer.
    pub async fn fast_forward(&mut self) -> Result<()> {
        let stream = self.names.stream.clone();
        let a = &mut self.admin;
        a.sql("BEGIN").await?;
        match a
            .sql(&format!("SELECT count(*) FROM {stream} WITH CONSUME"))
            .await
        {
            Ok(_) => a.sql("COMMIT").await.map(|_| ()),
            Err(e) => {
                let _ = a.sql("ROLLBACK").await;
                Err(e)
            }
        }
    }

    /// An empty consuming read: ROLLBACK keeps the Stream (and the local restart marker)
    /// where they are. A failed ROLLBACK leaves the session uncertain and fails closed.
    pub async fn rollback_empty(&mut self) -> Result<()> {
        ensure!(
            !self.capture_failed,
            "capture failed closed; rebuild required"
        );
        self.capture_active()?;
        self.capture_failed = true;
        self.capture.sql("ROLLBACK").await?;
        self.capture_failed = false;
        self.bootstrap_started = None;
        self.sequence += 1;
        Ok(())
    }

    fn capture_active(&self) -> Result<()> {
        ensure!(
            self.capture
                .session
                .get("txn_state")
                .and_then(Value::as_str)
                == Some("Active"),
            "capture transaction not Active"
        );
        Ok(())
    }

    async fn begin_snapshot(&mut self) -> Result<()> {
        ensure!(
            self.sequence == 0 && self.bootstrap_started.is_none() && !self.capture_failed,
            "snapshot already started or capture already committed"
        );
        let started = Instant::now();
        self.capture_failed = true;
        self.capture.sql("BEGIN").await?;
        self.capture_active()?;
        self.bootstrap_started = Some(started);
        self.capture_failed = false;
        Ok(())
    }

    async fn begin_capture_transaction(&mut self) -> Result<()> {
        ensure!(
            !self.capture_failed,
            "capture failed closed; rebuild required"
        );
        if self.bootstrap_started.is_none() {
            ensure!(
                self.sequence > 0,
                "snapshot transaction required before capture"
            );
            self.capture.sql("BEGIN").await?;
        }
        // A lost/expired bootstrap session must never fall back to a new transaction:
        // that would detach consume from the already loaded snapshot boundary.
        self.capture_active()
    }

    /// Check the source schema and create this instance's Stream unless it already exists.
    pub async fn prepare(&mut self) -> Result<()> {
        self.prepare_inner(false).await
    }

    /// Discover each table independently; reject unsupported types BEFORE creating its
    /// Stream. The same capture/transaction implementation serves legacy and real schemas.
    pub async fn prepare_schema(&mut self) -> Result<Arc<SourceSchema>> {
        ensure!(!self.oracle, "dynamic schemas are production mode only");
        self.prepare_inner(true).await?;
        self.schema
            .clone()
            .context("source schema missing after discovery")
    }

    async fn prepare_inner(&mut self, dynamic: bool) -> Result<()> {
        let n = self.names.clone();
        let a = &mut self.admin;
        let schema = a.sql(&format!("DESCRIBE {}", n.source)).await?;
        self.default_role = a
            .session
            .get("role")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let actual: Vec<_> = schema
            .iter()
            .map(|r| {
                (
                    r.first().and_then(Value::as_str).unwrap_or_default(),
                    r.get(1).and_then(Value::as_str).unwrap_or_default(),
                    r.get(2).and_then(Value::as_str).unwrap_or_default(),
                )
            })
            .collect();
        if dynamic {
            let (database, table) = n.source.split_once('.').context("qualified source name")?;
            self.schema = Some(Arc::new(SourceSchema::from_describe(
                database, table, &schema,
            )?));
        } else {
            ensure!(
                actual == EXPECTED_SCHEMA,
                "source schema must be id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL"
            );
        }
        // Reuse this instance's Stream without DROP. The snapshot AND first consume below
        // share a native transaction: pinned Stream/source metadata plus the native stream
        // version CAS at COMMIT protect against an old consumer's late COMMIT on restart.
        let (database, stream) = n.stream.split_once('.').context("qualified stream name")?;
        let existing = a
            .sql(&format!(
                "SELECT name FROM system.streams WHERE database = '{database}' AND name = '{stream}'"
            ))
            .await?;
        if existing.is_empty() {
            a.sql(&format!(
                "CREATE STREAM {} ON TABLE {} APPEND_ONLY = false",
                n.stream, n.source
            ))
            .await?;
        }
        Ok(())
    }

    /// Stream the fixed snapshot the Stream starts from into the engine, page by page.
    /// Oracle mode also returns (and keeps) the rows; it requires unique ids.
    pub async fn snapshot(&mut self, loader: &mut Loader) -> Result<Option<Vec<SourceRow>>> {
        self.begin_snapshot().await?;
        match self.snapshot_in_transaction(loader).await {
            Ok(rows) => Ok(rows),
            Err(error) => Err(self.abort_with(error).await),
        }
    }

    async fn snapshot_in_transaction(
        &mut self,
        loader: &mut Loader,
    ) -> Result<Option<Vec<SourceRow>>> {
        let n = self.names.clone();
        let oracle = self.oracle;
        let order = if oracle { " ORDER BY id" } else { "" };
        let mut kept: Vec<SourceRow> = Vec::new();
        let mut pages = self
            .capture
            .pages(
                &format!(
                    "SELECT id, payload, score, note FROM {} AT (STREAM => {}){order}",
                    n.source, n.stream
                ),
                PAGE_ROWS,
            )
            .await?;
        let read = async {
            while let Some(page) = pages.next().await? {
                let rows = page
                    .iter()
                    .map(|cells| parse_row(cells))
                    .collect::<Result<Vec<_>>>()?;
                if oracle {
                    kept.extend(rows.iter().cloned());
                    ensure!(
                        kept.len() <= MAX_ROWS,
                        "oracle mode is limited to {MAX_ROWS} initial rows"
                    );
                }
                loader.load(&rows).await?;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        match read {
            Ok(()) => {
                pages.close().await?;
            }
            Err(error) => return Err(pages.abort(error).await),
        }
        let fixed = self
            .capture
            .sql(&format!(
                "SELECT count(*) FROM {} AT (STREAM => {})",
                n.source, n.stream
            ))
            .await?;
        let fixed: usize = int(
            fixed
                .first()
                .and_then(|r| r.first())
                .unwrap_or(&Value::Null),
            "count",
        )?
        .try_into()?;
        ensure!(
            loader.loaded() == fixed,
            "fixed snapshot row count mismatch: loaded {}, source {fixed}",
            loader.loaded()
        );
        self.capture_active()?;
        self.rows = fixed as u64;
        if !oracle {
            return Ok(None);
        }
        ensure!(
            kept.windows(2).all(|w| w[0].id < w[1].id),
            "oracle mode requires unique source ids"
        );
        ensure!(
            !kept.is_empty(),
            "oracle mode requires a nonempty initial table"
        );
        self.state = kept.iter().map(|r| (r.id, r.clone())).collect();
        self.known = self.state.keys().copied().collect();
        Ok(Some(kept))
    }

    /// Snapshot a discovered source directly into an existing native base table. Caller
    /// owns checkpoint/persistence/reader fencing; no ReadySet instance is started here.
    pub async fn snapshot_into(&mut self, table: &mut Table) -> Result<u64> {
        self.begin_snapshot().await?;
        match self.snapshot_into_transaction(table).await {
            Ok(rows) => Ok(rows),
            Err(error) => Err(self.abort_with(error).await),
        }
    }

    async fn snapshot_into_transaction(&mut self, table: &mut Table) -> Result<u64> {
        let schema = self
            .schema
            .as_ref()
            .context("discover schema before snapshot")?;
        let sql = format!(
            "SELECT {} FROM {} AT (STREAM => {})",
            schema.column_list(),
            self.names.source,
            self.names.stream
        );
        let mut pages = self.capture.pages(&sql, PAGE_ROWS).await?;
        let mut loaded = 0u64;
        let result = async {
            while let Some(page) = pages.next().await? {
                let rows = page
                    .iter()
                    .map(|r| schema.decode_row(r))
                    .collect::<Result<Vec<_>>>()?;
                loaded += rows.len() as u64;
                for chunk in rows.chunks(10000) {
                    table.insert_many(chunk.iter().cloned()).await?;
                }
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = result {
            return Err(pages.abort(error).await);
        }
        pages.close().await?;
        let count = self
            .capture
            .sql(&format!(
                "SELECT count(*) FROM {} AT (STREAM => {})",
                self.names.source, self.names.stream
            ))
            .await?;
        let expected = count
            .first()
            .and_then(|r| r.first())
            .and_then(Value::as_str)
            .context("snapshot count missing")?
            .parse::<u64>()?;
        ensure!(
            loaded == expected,
            "snapshot row count mismatch: loaded {loaded}, expected {expected}"
        );
        self.capture_active()?;
        self.rows = loaded;
        Ok(loaded)
    }

    /// Production mode: BEGIN, then open a consuming read of the WHOLE Stream (never filtered
    /// or limited: a consuming read advances past everything it covers). Read every page,
    /// `Pages::close` it, apply, then `commit_consumed`. On any error call `abort_capture`.
    pub async fn begin_consume(&mut self) -> Result<Pages<'_>> {
        ensure!(!self.oracle, "oracle mode uses begin_capture");
        // Before BEGIN: schema drift must not consume or partly apply a changed source.
        // The multi-table coordinator can isolate this source without changing native DDL.
        self.check_schema().await?;
        let columns = self
            .schema
            .as_ref()
            .map(|s| s.column_list())
            .unwrap_or_else(|| "id, payload, score, note".into());
        let sql = format!(
            "SELECT {columns}, change$action, change$row_id, change$is_update FROM {} WITH CONSUME",
            self.names.stream
        );
        self.begin_capture_transaction().await?;
        self.capture.pages(&sql, PAGE_ROWS).await
    }

    pub async fn check_schema(&mut self) -> Result<()> {
        if let Some(schema) = self.schema.clone() {
            let rows = self
                .admin_read(&format!("DESCRIBE {}", schema.qualified()))
                .await?;
            let current = SourceSchema::from_describe(&schema.database, &schema.table, &rows)?;
            ensure!(
                schema.columns == current.columns,
                "source schema changed; rebuild this table before resuming capture"
            );
        }
        Ok(())
    }

    /// A read-only metadata statement on the autocommit admin session (never the capture
    /// transaction), so repeating it cannot change anything. A transient upstream failure
    /// (see [`transient_upstream`]) is retried with backoff instead of failing the table:
    /// failing a batch after its rows were applied locally forces a full table rebuild.
    async fn admin_read(&mut self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let mut attempt = 0u32;
        loop {
            match self.admin.sql(sql).await {
                Ok(rows) => return Ok(rows),
                Err(e) if attempt < ADMIN_READ_RETRIES && transient_upstream(&e) => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(200 << attempt)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Production mode: the batch is applied in ReadySet; advance the Stream. Only call this
    /// after the consuming read closed successfully. An error here is uncertain (the Stream
    /// may or may not have advanced) and must stop capture.
    pub async fn commit_consumed(&mut self, inserts: u64, deletes: u64) -> Result<()> {
        ensure!(
            !self.capture_failed,
            "capture failed closed; rebuild required"
        );
        ensure!(
            self.capture
                .session
                .get("txn_state")
                .and_then(Value::as_str)
                == Some("Active"),
            "capture transaction not Active"
        );
        self.capture_failed = true;
        self.capture.sql("COMMIT").await?;
        self.capture_failed = false;
        self.bootstrap_started = None;
        self.committed_at = Instant::now();
        self.sequence += 1;
        self.rows = (self.rows + inserts).saturating_sub(deletes);
        Ok(())
    }

    /// Open a transaction and read (consume, pending commit) every change in the Stream,
    /// then check it against the previous state. Never filtered or limited: a consuming
    /// read advances the Stream past everything it covers.
    pub async fn begin_capture(&mut self) -> Result<Prepared> {
        let n = self.names.clone();
        self.begin_capture_transaction().await?;
        let tx = &mut self.capture;
        let read = async {
            let raw = tx
                .sql(&format!(
                    "SELECT id, payload, score, note, change$action, change$row_id, change$is_update FROM {} WITH CONSUME ORDER BY id, change$action, change$row_id",
                    n.stream
                ))
                .await?;
            ensure!(
                tx.session.get("txn_state").and_then(Value::as_str) == Some("Active"),
                "capture transaction not Active"
            );
            Ok::<_, anyhow::Error>(raw)
        }
        .await;
        let raw = match read {
            Ok(raw) => raw,
            Err(error) => return Err(self.abort_with(error).await),
        };
        let prepared = (|| {
            ensure!(raw.len() < MAX_BATCH, "batch exceeds bounded capacity");
            let changes = raw
                .iter()
                .map(|r| parse_change(r))
                .collect::<Result<Vec<_>>>()?;
            let mut new_state = self.state.clone();
            for c in changes.iter().filter(|c| c.action == Action::Delete) {
                ensure!(
                    new_state.remove(&c.row.id).as_ref() == Some(&c.row),
                    "before-image mismatch"
                );
            }
            for c in changes.iter().filter(|c| c.action == Action::Insert) {
                ensure!(
                    new_state.insert(c.row.id, c.row.clone()).is_none(),
                    "duplicate source key"
                );
            }
            ensure!(
                new_state.len() <= MAX_ROWS,
                "source exceeds phase 1 capacity"
            );
            Ok(Prepared {
                batch: Batch {
                    generation: n.generation.clone(),
                    sequence: self.sequence + 1,
                    row_count: changes.len(),
                    changes_sha256: checksum(&changes)?,
                    changes,
                },
                new_state,
            })
        })();
        match prepared {
            Ok(prepared) => Ok(prepared),
            Err(error) => Err(self.abort_with(error).await),
        }
    }

    async fn abort_with(&mut self, error: anyhow::Error) -> anyhow::Error {
        // After aborting bootstrap, its already loaded native snapshot may no longer be
        // paired with a future Stream read. Never silently retry in a fresh transaction.
        self.capture_failed |= self.bootstrap_started.is_some();
        if matches!(
            self.capture
                .session
                .get("txn_state")
                .and_then(Value::as_str),
            Some("Active" | "Fail")
        ) && let Err(cleanup) = self.capture.sql("ROLLBACK").await
        {
            return error.context(format!("rollback also failed: {cleanup:#}"));
        }
        self.bootstrap_started = None;
        error
    }

    /// The batch could not be applied: keep its changes in the Stream.
    pub async fn abort_capture(&mut self, error: anyhow::Error) -> anyhow::Error {
        self.abort_with(error).await
    }

    /// Keys the barrier must prove absent, after recording every key ever seen.
    pub fn witness(&mut self, prepared: &Prepared) -> Result<(Vec<SourceRow>, Vec<i64>)> {
        self.known.extend(prepared.new_state.keys().copied());
        ensure!(
            self.known.len() <= MAX_KNOWN_KEYS,
            "historical key limit reached"
        );
        let mut missing: Vec<i64> = self
            .known
            .iter()
            .copied()
            .filter(|k| !prepared.new_state.contains_key(k))
            .collect();
        missing.sort_unstable();
        Ok((prepared.new_state.values().cloned().collect(), missing))
    }

    /// The batch is applied in ReadySet: advance the Stream. An error here is uncertain
    /// (the Stream may or may not have advanced) and must stop capture.
    pub async fn commit_capture(&mut self, prepared: Prepared) -> Result<()> {
        ensure!(
            !self.capture_failed,
            "capture failed closed; rebuild required"
        );
        self.capture_active()?;
        self.capture_failed = true;
        self.capture.sql("COMMIT").await?;
        self.capture_failed = false;
        self.bootstrap_started = None;
        self.sequence = prepared.batch.sequence;
        self.state = prepared.new_state;
        self.rows = self.state.len() as u64;
        Ok(())
    }
}

#[cfg(test)]
#[path = "replicator_boundary_tests.rs"]
mod boundary_tests;
