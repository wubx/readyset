//! Restart marker stored as each base table's native ReadySet replication offset.
//!
//! The marker lives in the base table's own RocksDB (synchronous WAL) and records how the
//! local rows relate to the table's Databend Stream position (`system.streams.table_version`,
//! the source table's meta seq at the Stream offset):
//!
//! - `Applied(v)`: local rows equal the source at Stream version `v`.
//! - `Applying(p)`: local rows may contain an unfinished batch on top of version `p`. A batch
//!   is COMMITted only after every native write ACKed, so if the Stream has moved past `p`
//!   (only this instance consumes it) the batch is complete; otherwise it is not.
//! - `Rebuilding`: a snapshot load is in progress; local rows are unusable.
//!
//! Encoded in `DatabendPosition` without native changes: generation =
//! `{schema digest}_{a|p|r}{version}`, sequence = a counter that grows on every write. The
//! native base drops an offset only if it compares `<=` within ONE generation, so a phase
//! change (new generation) or a larger counter is always stored.
use anyhow::{Result, bail, ensure};
use replication_offset::ReplicationOffset;
use replication_offset::databend::DatabendPosition;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Rebuilding,
    Applying(u64),
    Applied(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Marker {
    pub digest: String,
    pub phase: Phase,
    pub counter: u64,
}

impl Marker {
    pub fn offset(&self) -> Result<ReplicationOffset> {
        let phase = match self.phase {
            Phase::Rebuilding => "r".to_string(),
            Phase::Applying(v) => format!("p{v}"),
            Phase::Applied(v) => format!("a{v}"),
        };
        Ok(ReplicationOffset::Databend(DatabendPosition::new(
            format!("{}_{phase}", self.digest),
            self.counter,
        )?))
    }

    pub fn parse(offset: &ReplicationOffset) -> Result<Self> {
        let ReplicationOffset::Databend(position) = offset else {
            bail!("non-Databend restart marker");
        };
        let Some((digest, phase)) = position.generation().rsplit_once('_') else {
            bail!("malformed restart marker");
        };
        ensure!(!digest.is_empty(), "malformed restart marker");
        let version = |s: &str| -> Result<u64> { Ok(s.parse()?) };
        let phase = match phase.split_at_checked(1) {
            Some(("r", "")) => Phase::Rebuilding,
            Some(("p", v)) => Phase::Applying(version(v)?),
            Some(("a", v)) => Phase::Applied(version(v)?),
            _ => bail!("malformed restart marker phase"),
        };
        Ok(Self {
            digest: digest.into(),
            phase,
            counter: position.sequence(),
        })
    }

    /// The next marker for this table: same digest, strictly larger counter.
    pub fn next(&self, phase: Phase) -> Self {
        Self {
            digest: self.digest.clone(),
            phase,
            counter: self.counter + 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// No local base table: create it and load a snapshot.
    Create,
    /// Local rows equal the source at this Stream version: continue consuming.
    Resume(u64),
    /// Local rows cannot be trusted: drop the base table, recreate, snapshot.
    Rebuild(&'static str),
}

/// Restart decision for one table. `stream_version` is read from Databend after the
/// Stream is ensured to exist; nobody but this instance consumes that Stream.
pub fn decide(
    table_exists: bool,
    marker: Option<&Marker>,
    digest: &str,
    stream_version: u64,
) -> Decision {
    if !table_exists {
        return Decision::Create;
    }
    let Some(marker) = marker else {
        return Decision::Rebuild("no restart marker");
    };
    if marker.digest != digest {
        return Decision::Rebuild("source schema changed");
    }
    match marker.phase {
        Phase::Applied(v) if v == stream_version => Decision::Resume(v),
        Phase::Applied(_) => Decision::Rebuild("stream moved without a local apply"),
        // COMMIT only follows complete native ACKs, so a moved Stream proves completion.
        Phase::Applying(p) if p != stream_version => Decision::Resume(stream_version),
        Phase::Applying(_) => Decision::Rebuild("interrupted batch was not committed"),
        Phase::Rebuilding => Decision::Rebuild("interrupted snapshot load"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(phase: Phase, counter: u64) -> Marker {
        Marker {
            digest: "0123456789ab".into(),
            phase,
            counter,
        }
    }

    #[test]
    fn markers_round_trip_through_native_offsets() {
        for phase in [
            Phase::Rebuilding,
            Phase::Applying(7),
            Phase::Applied(u64::MAX),
        ] {
            let m = marker(phase, 42);
            assert_eq!(Marker::parse(&m.offset().unwrap()).unwrap(), m);
        }
        let bad = ReplicationOffset::Databend(DatabendPosition::new("nophase", 1).unwrap());
        assert!(Marker::parse(&bad).is_err());
        let bad = ReplicationOffset::Databend(DatabendPosition::new("d_x1", 1).unwrap());
        assert!(Marker::parse(&bad).is_err());
    }

    #[test]
    fn every_next_marker_is_stored_by_the_native_base() {
        // The base drops an offset only when new <= current inside one generation.
        let a = marker(Phase::Applied(10), 5);
        let p = a.next(Phase::Applying(10));
        let back = p.next(Phase::Applied(10));
        for (old, new) in [(&a, &p), (&p, &back)] {
            let (old, new) = (old.offset().unwrap(), new.offset().unwrap());
            assert!(!matches!(new.try_partial_cmp(&old), Ok(o) if o.is_le()));
        }
        let same = back.next(Phase::Applied(10));
        assert!(
            same.offset()
                .unwrap()
                .try_partial_cmp(&back.offset().unwrap())
                .unwrap()
                .is_gt()
        );
    }

    #[test]
    fn restart_decisions_cover_every_crash_window() {
        let d = "0123456789ab";
        assert_eq!(decide(false, None, d, 9), Decision::Create);
        assert!(matches!(decide(true, None, d, 9), Decision::Rebuild(_)));
        let mut other = marker(Phase::Applied(9), 1);
        other.digest = "ffffffffffff".into();
        assert!(matches!(
            decide(true, Some(&other), d, 9),
            Decision::Rebuild(_)
        ));
        // Clean stop / crash between batches.
        assert_eq!(
            decide(true, Some(&marker(Phase::Applied(9), 1)), d, 9),
            Decision::Resume(9)
        );
        assert!(matches!(
            decide(true, Some(&marker(Phase::Applied(8), 1)), d, 9),
            Decision::Rebuild(_)
        ));
        // Crash mid-batch, or COMMIT never executed: rebuild only this table.
        assert!(matches!(
            decide(true, Some(&marker(Phase::Applying(9), 1)), d, 9),
            Decision::Rebuild(_)
        ));
        // COMMIT executed but its response or the applied marker was lost.
        assert_eq!(
            decide(true, Some(&marker(Phase::Applying(8), 1)), d, 9),
            Decision::Resume(9)
        );
        assert!(matches!(
            decide(true, Some(&marker(Phase::Rebuilding, 1)), d, 9),
            Decision::Rebuild(_)
        ));
    }
}
