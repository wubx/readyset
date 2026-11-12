//! Position in a single Databend outbox generation, NOT a native snapshot ID or WAL offset.
use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use readyset_errors::{ReadySetResult, replication_failed};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabendPosition {
    generation: String,
    sequence: u64,
}

impl DatabendPosition {
    pub fn new(generation: impl Into<String>, sequence: u64) -> ReadySetResult<Self> {
        let generation = generation.into();
        if generation.is_empty()
            || generation.len() > 256
            || !generation
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        {
            replication_failed!("Invalid Databend outbox generation");
        }
        Ok(Self {
            generation,
            sequence,
        })
    }

    pub fn generation(&self) -> &str {
        &self.generation
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl PartialOrd for DatabendPosition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.generation == other.generation).then(|| self.sequence.cmp(&other.sequence))
    }
}

impl fmt::Display for DatabendPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "databend:{}:{}", self.generation, self.sequence)
    }
}

impl FromStr for DatabendPosition {
    type Err = readyset_errors::ReadySetError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let Some((generation, sequence)) = input
            .strip_prefix("databend:")
            .and_then(|s| s.split_once(':'))
        else {
            replication_failed!("Invalid Databend outbox position");
        };
        let Ok(sequence) = sequence.parse() else {
            replication_failed!("Invalid Databend outbox sequence");
        };
        Self::new(generation, sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ReplicationOffset;

    #[test]
    fn compares_only_within_one_generation() {
        let a = DatabendPosition::new("g1", 1).unwrap();
        let b = DatabendPosition::new("g1", 2).unwrap();
        let other = DatabendPosition::new("g2", 1).unwrap();
        assert!(a < b);
        assert_eq!(a.partial_cmp(&other), None);
        let a = ReplicationOffset::Databend(a);
        assert!(
            a.try_partial_cmp(&ReplicationOffset::Databend(other))
                .is_err()
        );
    }

    #[test]
    fn display_json_and_bincode_roundtrip() {
        let value =
            ReplicationOffset::Databend(DatabendPosition::new("table_generation", 42).unwrap());
        assert_eq!(
            value.to_string().parse::<ReplicationOffset>().unwrap(),
            value
        );
        assert_eq!(
            serde_json::from_str::<ReplicationOffset>(&serde_json::to_string(&value).unwrap())
                .unwrap(),
            value
        );
        let bytes = bincode::serialize(&value).unwrap();
        assert_eq!(&bytes[..4], &3_u32.to_le_bytes()); // Appended after all existing variants.
        assert_eq!(
            bincode::deserialize::<ReplicationOffset>(&bytes).unwrap(),
            value
        );
        assert_eq!(value.transaction_start(), value);
        assert_eq!(value.dialect(), readyset_sql::Dialect::MySQL); // Local normalized schema only.
    }

    #[test]
    fn invalid_text_is_rejected() {
        for input in [
            "databend::1",
            "databend:a/b:1",
            "databend:g:-1",
            "databend:g:1:2",
        ] {
            assert!(input.parse::<ReplicationOffset>().is_err(), "{input}");
        }
    }

    #[test]
    fn existing_bincode_variant_numbers_are_unchanged() {
        let mysql = ReplicationOffset::MySql(
            crate::mysql::MySqlPosition::from_file_name_and_position("binlog.000001".into(), 4)
                .unwrap(),
        );
        let gtid = ReplicationOffset::Gtid(crate::GtidSet::default());
        let pg = ReplicationOffset::Postgres(crate::postgres::PostgresPosition {
            commit_lsn: 1.into(),
            lsn: 1.into(),
        });
        for (index, value) in [mysql, gtid, pg].into_iter().enumerate() {
            assert_eq!(
                &bincode::serialize(&value).unwrap()[..4],
                &(index as u32).to_le_bytes()
            );
        }
    }
}
