//! Databend Stream change capture for one fixed-schema table.
//!
//! The only Databend object is one Stream per synchronized table, next to the table. Each
//! round runs `BEGIN; SELECT ... FROM stream WITH CONSUME; <apply to ReadySet>; COMMIT`:
//! the Stream only advances when the transaction commits, so a failed apply leaves the
//! changes in the Stream. Tables, cache declarations and progress live in ReadySet.
//!
//! Production mode streams the snapshot and every batch page by page and keeps no copy of
//! the table. Oracle mode (POC regression verifiers) also keeps every row to re-verify the
//! replica after each batch, and is bounded to small tables with unique ids.
use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::engine::Loader;
use crate::upstream::{DatabendClient, Pages};
use crate::{Action, Batch, Change, SourceRow};

/// Oracle mode bounds (the engine re-verifies every row per batch).
pub const MAX_ROWS: usize = 9000;
pub const MAX_KNOWN_KEYS: usize = 12000;
const MAX_BATCH: usize = 10000;
/// Rows per HTTP page for snapshots and production batches (about 9 MB of JSON).
pub const PAGE_ROWS: usize = 50000;

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
        }
    }

    /// Check the source schema and create this instance's Stream unless it already exists.
    pub async fn prepare(&mut self) -> Result<()> {
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
        ensure!(
            actual == EXPECTED_SCHEMA,
            "source schema must be id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL"
        );
        // Reuse this instance's Stream when it already exists (restart with the same
        // instance_id): the snapshot below is read AT the Stream's offset, so snapshot +
        // the changes still in the Stream is exactly the current table. No DROP is needed,
        // which keeps the account at SELECT + CREATE (DROP would be database-wide).
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
        let n = self.names.clone();
        let oracle = self.oracle;
        let order = if oracle { " ORDER BY id" } else { "" };
        let mut kept: Vec<SourceRow> = Vec::new();
        let mut pages = self
            .admin
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
            .admin
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

    /// Production mode: BEGIN, then open a consuming read of the WHOLE Stream (never filtered
    /// or limited: a consuming read advances past everything it covers). Read every page,
    /// `Pages::close` it, apply, then `commit_consumed`. On any error call `abort_capture`.
    pub async fn begin_consume(&mut self) -> Result<Pages<'_>> {
        ensure!(!self.oracle, "oracle mode uses begin_capture");
        let sql = format!(
            "SELECT id, payload, score, note, change$action, change$row_id, change$is_update FROM {} WITH CONSUME",
            self.names.stream
        );
        self.capture.sql("BEGIN").await?;
        self.capture.pages(&sql, PAGE_ROWS).await
    }

    /// Production mode: the batch is applied in ReadySet; advance the Stream. Only call this
    /// after the consuming read closed successfully. An error here is uncertain (the Stream
    /// may or may not have advanced) and must stop capture.
    pub async fn commit_consumed(&mut self, inserts: u64, deletes: u64) -> Result<()> {
        ensure!(
            self.capture
                .session
                .get("txn_state")
                .and_then(Value::as_str)
                == Some("Active"),
            "capture transaction not Active"
        );
        self.capture.sql("COMMIT").await?;
        self.sequence += 1;
        self.rows = (self.rows + inserts).saturating_sub(deletes);
        Ok(())
    }

    /// Open a transaction and read (consume, pending commit) every change in the Stream,
    /// then check it against the previous state. Never filtered or limited: a consuming
    /// read advances the Stream past everything it covers.
    pub async fn begin_capture(&mut self) -> Result<Prepared> {
        let n = self.names.clone();
        let tx = &mut self.capture;
        tx.sql("BEGIN").await?;
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
        self.capture.sql("COMMIT").await?;
        self.sequence = prepared.batch.sequence;
        self.state = prepared.new_state;
        self.rows = self.state.len() as u64;
        Ok(())
    }
}
