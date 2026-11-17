//! Databend Stream change types and the strictly serial single-table ReadySet sink.
//! No native Databend SQL frontend and no distributed consumer ownership protocol.
pub mod cache_control;
pub mod declaration;
pub mod engine;
mod engine_fence;
pub mod forward_scope;
pub mod native_query;
pub mod reader_fence;
pub mod replicator;
pub mod restart_marker;
pub mod shared_engine;
pub mod source_schema;
pub mod upstream;

use std::collections::HashSet;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use readyset_client::{PersistencePoint, ReadySetHandle, Table, TableOperation};
use readyset_data::{Collation, DfValue};
use replication_offset::ReplicationOffset;
use replication_offset::databend::DatabendPosition;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRow {
    pub id: i64,
    pub payload: String,
    pub score: i64,
    pub note: Option<String>,
}
impl SourceRow {
    pub fn values(&self) -> Vec<DfValue> {
        vec![
            self.id.into(),
            DfValue::from_str_and_collation(&self.payload, Collation::Utf8Binary),
            self.score.into(),
            self.note
                .as_deref()
                .map(|s| DfValue::from_str_and_collation(s, Collation::Utf8Binary))
                .unwrap_or(DfValue::None),
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Action {
    Delete,
    Insert,
}

// Field order defines the compact UTF-8 JSON checksum contract with the capture script.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Change {
    pub action: Action,
    pub row_id: String,
    pub is_update: bool,
    pub row: SourceRow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub generation: String,
    pub sequence: u64,
    pub row_count: usize,
    pub changes_sha256: String,
    pub changes: Vec<Change>,
}

impl Batch {
    pub fn validate(&self) -> Result<()> {
        DatabendPosition::new(&self.generation, self.sequence)?;
        ensure!(
            self.sequence > 0,
            "sequence zero is reserved for the initial snapshot"
        );
        ensure!(
            self.row_count == self.changes.len(),
            "incomplete batch: row_count mismatch"
        );
        ensure!(
            self.changes.len() <= 10000,
            "M2 batch exceeds 10000 operations; splitting not implemented"
        );
        let digest = hex::encode(Sha256::digest(serde_json::to_vec(&self.changes)?));
        ensure!(digest == self.changes_sha256, "batch checksum mismatch");
        let mut inserts = HashSet::new();
        let mut deletes = HashSet::new();
        for change in &self.changes {
            ensure!(!change.row_id.is_empty(), "missing change$row_id");
            let keys = match change.action {
                Action::Insert => &mut inserts,
                Action::Delete => &mut deletes,
            };
            ensure!(
                keys.insert(change.row.id),
                "multiple same-action changes for one business key; source is not unique"
            );
        }
        Ok(())
    }

    pub fn position(&self) -> Result<ReplicationOffset> {
        Ok(ReplicationOffset::Databend(DatabendPosition::new(
            &self.generation,
            self.sequence,
        )?))
    }

    pub fn operations(&self) -> Result<Vec<TableOperation>> {
        self.validate()?;
        let mut operations = Vec::with_capacity(self.changes.len() + 1);
        // Stream rows are unordered. Withdraw ALL old rows before inserting new rows,
        // including updates that move/swap a business key.
        for action in [Action::Delete, Action::Insert] {
            for change in self.changes.iter().filter(|c| c.action == action) {
                operations.push(match action {
                    Action::Delete => TableOperation::DeleteRow {
                        row: change.row.values(),
                    },
                    Action::Insert => TableOperation::Insert(change.row.values()),
                });
            }
        }
        operations.push(TableOperation::SetReplicationOffset(self.position()?));
        Ok(operations)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Delivery {
    Apply,
    Skip,
}

pub fn delivery(current: &DatabendPosition, batch: &Batch) -> Result<Delivery> {
    batch.validate()?;
    ensure!(
        current.generation() == batch.generation,
        "outbox generation mismatch"
    );
    if batch.sequence <= current.sequence() {
        return Ok(Delivery::Skip);
    }
    ensure!(
        current.sequence().checked_add(1) == Some(batch.sequence),
        "batch gap: refusing out-of-order delivery"
    );
    Ok(Delivery::Apply)
}

/// One owner, one table, one outstanding apply. No multi-consumer guarantees are implied.
/// Always consult engine state instead of treating an in-memory counter as a checkpoint.
pub struct Sink {
    generation: String,
    table_name: String,
}
impl Sink {
    pub fn new(generation: String, table_name: String) -> Result<Self> {
        DatabendPosition::new(&generation, 0)?;
        Ok(Self {
            generation,
            table_name,
        })
    }

    pub async fn checkpoint(&self, controller: &mut ReadySetHandle) -> Result<DatabendPosition> {
        let offsets = controller.replication_offsets().await?;
        let offset = offsets
            .tables
            .iter()
            .find(|(name, _)| name.name.as_str() == self.table_name)
            .and_then(|(_, offset)| offset.as_ref())
            .context("table checkpoint missing")?;
        let ReplicationOffset::Databend(position) = offset else {
            bail!("non-Databend checkpoint");
        };
        ensure!(
            position.generation() == self.generation,
            "checkpoint generation mismatch"
        );
        Ok(position.clone())
    }

    pub async fn wait_persisted(&self, controller: &mut ReadySetHandle, target: u64) -> Result<()> {
        self.wait_persisted_for(controller, target, Duration::from_secs(30))
            .await
    }

    /// As [`Sink::wait_persisted`], for bulk loads and large batches that need longer.
    pub async fn wait_persisted_for(
        &self,
        controller: &mut ReadySetHandle,
        target: u64,
        limit: Duration,
    ) -> Result<()> {
        let mut observed = String::from("no checkpoint observed");
        tokio::time::timeout(limit, async {
            loop {
                let current = self.checkpoint(controller).await?;
                let persisted = controller.min_persisted_replication_offset().await?;
                observed = format!("checkpoint={current}, persisted={persisted:?}");
                // UpTo is a low watermark at the START of unpersisted work. In particular
                // UpTo(target) is not proof that batch target has been synced. This single-
                // table, serial experiment waits for all base writes to be persisted instead.
                if current.sequence() >= target && matches!(persisted, PersistencePoint::Persisted)
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .with_context(|| format!("timed out waiting for persistent batch {target}: {observed}"))?
    }

    pub async fn apply(
        &mut self,
        controller: &mut ReadySetHandle,
        table: &mut Table,
        batch: &Batch,
    ) -> Result<Value> {
        let started = Instant::now();
        let current = self.checkpoint(controller).await?;
        let skipped = delivery(&current, batch)? == Delivery::Skip;
        if !skipped {
            // Exactly one Table RPC for this bounded batch, containing rows + checkpoint.
            // An uncertain outcome is returned to the caller, never blindly retried here.
            table.perform_all(batch.operations()?).await?;
        }
        self.wait_persisted(
            controller,
            if skipped {
                current.sequence()
            } else {
                batch.sequence
            },
        )
        .await?;
        let after = self.checkpoint(controller).await?;
        Ok(
            json!({"ok": true, "skipped": skipped, "delivered_sequence": batch.sequence,
                  "checkpoint": after.sequence(), "rows": batch.row_count,
                  "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn batch(sequence: u64) -> Batch {
        let changes = vec![Change {
            action: Action::Insert,
            row_id: "r1".into(),
            is_update: false,
            row: SourceRow {
                id: 1,
                payload: "中文".into(),
                score: i64::MIN,
                note: None,
            },
        }];
        Batch {
            generation: "g".into(),
            sequence,
            row_count: changes.len(),
            changes_sha256: hex::encode(Sha256::digest(serde_json::to_vec(&changes).unwrap())),
            changes,
        }
    }
    #[test]
    fn checksum_contract_matches_python_utf8_and_int64() {
        assert_eq!(
            batch(1).changes_sha256,
            "87c722b5903e3437bb527d8de03464a79227340251c5dd3d56985e3c4f439c1b"
        );
    }
    #[test]
    fn duplicates_and_old_batches_skip_but_gaps_fail() {
        let current = DatabendPosition::new("g", 2).unwrap();
        assert_eq!(delivery(&current, &batch(1)).unwrap(), Delivery::Skip);
        assert_eq!(delivery(&current, &batch(2)).unwrap(), Delivery::Skip);
        assert_eq!(delivery(&current, &batch(3)).unwrap(), Delivery::Apply);
        assert!(delivery(&current, &batch(4)).is_err());
        assert!(delivery(&DatabendPosition::new("other", 2).unwrap(), &batch(2)).is_err());
    }
    #[test]
    fn incomplete_or_tampered_batch_rejected_even_when_old() {
        let mut b = batch(1);
        b.row_count += 1;
        assert!(b.validate().is_err());
        let mut b = batch(1);
        b.changes[0].row.score = 0;
        assert!(b.validate().is_err());
    }
    #[test]
    fn update_order_is_delete_then_insert_then_checkpoint() {
        let mut b = batch(1);
        let mut old = b.changes[0].clone();
        old.action = Action::Delete;
        old.row.payload = "old".into();
        b.changes.push(old);
        b.row_count = 2;
        b.changes_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&b.changes).unwrap()));
        let ops = b.operations().unwrap();
        assert!(matches!(ops[0], TableOperation::DeleteRow { .. }));
        assert!(matches!(ops[1], TableOperation::Insert(_)));
        assert!(matches!(ops[2], TableOperation::SetReplicationOffset(_)));
    }
    #[test]
    fn duplicate_source_key_rejected() {
        let mut b = batch(1);
        b.changes.push(b.changes[0].clone());
        b.row_count = 2;
        b.changes_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&b.changes).unwrap()));
        assert!(
            b.validate()
                .unwrap_err()
                .to_string()
                .contains("business key")
        );
    }
    #[test]
    fn empty_batch_still_carries_a_checkpoint() {
        let b = Batch {
            generation: "g".into(),
            sequence: 1,
            row_count: 0,
            changes_sha256: hex::encode(Sha256::digest(b"[]")),
            changes: vec![],
        };
        assert_eq!(b.operations().unwrap().len(), 1);
    }
}
