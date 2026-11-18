//! Restart marker stored as each base table's native ReadySet replication offset.
//!
//! The marker lives in the base table's own RocksDB (synchronous WAL) and records how the
//! local rows relate to the table's Databend Stream position: `stream` is the Stream's
//! identity (`system.streams.stream_id`) and versions are `system.streams.table_version`,
//! the source table's meta seq at the Stream offset.
//!
//! - `Applied(v)`: local rows equal the source at Stream version `v`.
//! - `Applying(p)`: local rows may contain an unfinished batch on top of version `p`, and its
//!   COMMIT has not been sent. If the Stream moved past `p`, someone else consumed it.
//! - `Committing(p)`: every native write of the batch on top of `p` ACKed and its COMMIT may
//!   have been sent. If THE SAME Stream has moved past `p`, the batch is complete (our
//!   COMMIT moved it; another consumer is caught by the per-round Stream check instead).
//! - `Rebuilding`: a snapshot load is in progress; local rows are unusable.
//!
//! A recreated Stream (new `stream_id`) proves nothing about earlier COMMITs: rebuild.
//!
//! Encoded in `DatabendPosition` without native changes: generation =
//! `{schema digest}_{stream id}_{a|p|r}{version}`, sequence = a counter that grows on every
//! write. The native base drops an offset only if it compares `<=` within ONE generation,
//! so a phase change (new generation) or a larger counter is always stored.
use anyhow::{Result, bail, ensure};
use replication_offset::ReplicationOffset;
use replication_offset::databend::DatabendPosition;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Rebuilding,
    Applying(u64),
    Committing(u64),
    Applied(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Marker {
    pub digest: String,
    pub stream: u64,
    pub phase: Phase,
    pub counter: u64,
}

impl Marker {
    pub fn offset(&self) -> Result<ReplicationOffset> {
        let phase = match self.phase {
            Phase::Rebuilding => "r".to_string(),
            Phase::Applying(v) => format!("p{v}"),
            Phase::Committing(v) => format!("c{v}"),
            Phase::Applied(v) => format!("a{v}"),
        };
        Ok(ReplicationOffset::Databend(DatabendPosition::new(
            format!("{}_{}_{phase}", self.digest, self.stream),
            self.counter,
        )?))
    }

    pub fn parse(offset: &ReplicationOffset) -> Result<Self> {
        let ReplicationOffset::Databend(position) = offset else {
            bail!("non-Databend restart marker");
        };
        let mut parts = position.generation().splitn(3, '_');
        let (Some(digest), Some(stream), Some(phase)) = (parts.next(), parts.next(), parts.next())
        else {
            bail!("malformed restart marker");
        };
        ensure!(
            !digest.is_empty() && digest.bytes().all(|b| b.is_ascii_hexdigit()),
            "malformed restart marker"
        );
        let version = |s: &str| -> Result<u64> { Ok(s.parse()?) };
        let phase = match phase.split_at_checked(1) {
            Some(("r", "")) => Phase::Rebuilding,
            Some(("p", v)) => Phase::Applying(version(v)?),
            Some(("c", v)) => Phase::Committing(version(v)?),
            Some(("a", v)) => Phase::Applied(version(v)?),
            _ => bail!("malformed restart marker phase"),
        };
        Ok(Self {
            digest: digest.into(),
            stream: version(stream)?,
            phase,
            counter: position.sequence(),
        })
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

/// Restart decision for one table. `stream`/`stream_version` are read from Databend after
/// the Stream is ensured to exist. Only a `Committing` marker may resume on a moved Stream:
/// an `Applying` batch never sent its COMMIT, so a move there is another consumer's.
pub fn decide(
    table_exists: bool,
    marker: Option<&Marker>,
    digest: &str,
    stream: u64,
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
    if marker.stream != stream {
        return Decision::Rebuild("stream recreated");
    }
    match marker.phase {
        Phase::Applied(v) if v == stream_version => Decision::Resume(v),
        Phase::Applied(_) => Decision::Rebuild("stream moved without a local apply"),
        Phase::Applying(p) if p != stream_version => {
            Decision::Rebuild("stream moved by another consumer")
        }
        // COMMIT only follows complete native ACKs, so a moved Stream proves completion.
        Phase::Committing(p) if p != stream_version => Decision::Resume(stream_version),
        Phase::Applying(_) | Phase::Committing(_) => {
            Decision::Rebuild("interrupted batch was not committed")
        }
        Phase::Rebuilding => Decision::Rebuild("interrupted snapshot load"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(phase: Phase, counter: u64) -> Marker {
        Marker {
            digest: "0123456789ab".into(),
            stream: 77,
            phase,
            counter,
        }
    }

    #[test]
    fn markers_round_trip_through_native_offsets() {
        for phase in [
            Phase::Rebuilding,
            Phase::Applying(7),
            Phase::Committing(8),
            Phase::Applied(u64::MAX),
        ] {
            let m = marker(phase, 42);
            assert_eq!(Marker::parse(&m.offset().unwrap()).unwrap(), m);
        }
        for bad in ["nophase", "d_x1", "0a_a1", "0a_1_x1", "zz_1_a1"] {
            let bad = ReplicationOffset::Databend(DatabendPosition::new(bad, 1).unwrap());
            assert!(Marker::parse(&bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn every_next_marker_is_stored_by_the_native_base() {
        // The base drops an offset only when new <= current inside one generation.
        let a = marker(Phase::Applied(10), 5);
        let next = |m: &Marker, phase| Marker {
            phase,
            counter: m.counter + 1,
            ..m.clone()
        };
        let p = next(&a, Phase::Applying(10));
        let c = next(&p, Phase::Committing(10));
        let back = next(&c, Phase::Applied(10));
        for (old, new) in [(&a, &p), (&p, &c), (&c, &back)] {
            let (old, new) = (old.offset().unwrap(), new.offset().unwrap());
            assert!(!matches!(new.try_partial_cmp(&old), Ok(o) if o.is_le()));
        }
        let same = next(&back, Phase::Applied(10));
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
        let decide = |exists, m: Option<&Marker>, v| decide(exists, m, d, 77, v);
        assert_eq!(decide(false, None, 9), Decision::Create);
        assert!(matches!(decide(true, None, 9), Decision::Rebuild(_)));
        let mut other = marker(Phase::Applied(9), 1);
        other.digest = "ffffffffffff".into();
        assert!(matches!(
            decide(true, Some(&other), 9),
            Decision::Rebuild(_)
        ));
        // Clean stop / crash between batches.
        assert_eq!(
            decide(true, Some(&marker(Phase::Applied(9), 1)), 9),
            Decision::Resume(9)
        );
        assert!(matches!(
            decide(true, Some(&marker(Phase::Applied(8), 1)), 9),
            Decision::Rebuild(_)
        ));
        // Crash mid-batch, or COMMIT never executed: rebuild only this table.
        for phase in [Phase::Applying(9), Phase::Committing(9)] {
            assert_eq!(
                decide(true, Some(&marker(phase, 1)), 9),
                Decision::Rebuild("interrupted batch was not committed")
            );
        }
        // COMMIT executed but its response or the applied marker was lost.
        assert_eq!(
            decide(true, Some(&marker(Phase::Committing(8), 1)), 9),
            Decision::Resume(9)
        );
        // The batch never sent its COMMIT, yet the Stream moved: another consumer took
        // changes this instance never applied.
        assert_eq!(
            decide(true, Some(&marker(Phase::Applying(8), 1)), 9),
            Decision::Rebuild("stream moved by another consumer")
        );
        assert!(matches!(
            decide(true, Some(&marker(Phase::Rebuilding, 1)), 9),
            Decision::Rebuild(_)
        ));
    }

    #[test]
    fn recreated_stream_never_resumes_an_interrupted_batch() {
        // Crash mid-batch at version 8, then the Stream is dropped and recreated (new id,
        // new version). The moved version must NOT be read as "our COMMIT happened".
        let m = marker(Phase::Committing(8), 3);
        assert_eq!(
            decide(true, Some(&m), "0123456789ab", 78, 9),
            Decision::Rebuild("stream recreated")
        );
        let m = marker(Phase::Applied(9), 3);
        assert_eq!(
            decide(true, Some(&m), "0123456789ab", 78, 9),
            Decision::Rebuild("stream recreated")
        );
    }
}
