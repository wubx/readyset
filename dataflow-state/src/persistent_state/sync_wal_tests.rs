//! Regression for SharedEngine's native sync-WAL premise, not a power-loss simulation.
use super::*;
use replication_offset::databend::DatabendPosition;

fn open(dir: &std::path::Path, name: &str) -> PersistentState {
    let mut state = PersistentState::new(
        name.into(),
        None,
        Vec::<Box<[usize]>>::new(),
        &PersistenceParameters {
            mode: DurabilityMode::Permanent,
            storage_dir: Some(dir.into()),
            wal_flush_interval_seconds: 0,
            ..Default::default()
        },
        PersistenceType::BaseTable,
        None,
    )
    .unwrap();
    state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
    state
}
fn offset(sequence: u64) -> ReplicationOffset {
    ReplicationOffset::Databend(DatabendPosition::new("native_sync_wal", sequence).unwrap())
}
fn stored(state: &PersistentState) -> Vec<Vec<DfValue>> {
    let mut all = state.all_records();
    let mut rows: Vec<_> = all.read().iter().collect();
    rows.sort();
    rows
}

#[test]
fn separate_chunks_and_offset_survive_exit_without_state_drop() {
    let dir = tempfile::tempdir().unwrap();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "persistent_state::sync_wal_tests::native_sync_wal_child",
            "--nocapture",
        ])
        .env("READYSET_NATIVE_SYNC_WAL_TEST_DIR", dir.path())
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "child: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let a = open(dir.path(), "source_a");
    let b = open(dir.path(), "source_b");
    // A's partial batch is durable but MUST NOT acquire a completed-batch offset.
    assert_eq!(a.replication_offset(), Some(&offset(0)));
    assert_eq!(stored(&a), vec![vec![1.into(), "pending".into()]; 2]);
    assert_eq!(b.replication_offset(), Some(&offset(7)));
    let mut expected = vec![vec![2.into(), "old".into()], vec![2.into(), "new".into()]];
    expected.sort();
    assert_eq!(stored(&b), expected);
}

#[test]
#[ignore = "subprocess helper; invoked by separate_chunks_and_offset_survive_exit_without_state_drop"]
fn native_sync_wal_child() {
    let dir = PathBuf::from(std::env::var("READYSET_NATIVE_SYNC_WAL_TEST_DIR").unwrap());
    let mut a = open(&dir, "source_a");
    let mut b = open(&dir, "source_b");
    for state in [&mut a, &mut b] {
        state
            .process_records(&mut Records::default(), None, Some(offset(0)))
            .unwrap();
    }
    let pending = vec![1.into(), "pending".into()];
    a.process_records(
        &mut vec![(pending.clone(), true), (pending, true)].into(),
        None,
        None,
    )
    .unwrap();
    let old = vec![2.into(), "old".into()];
    b.process_records(
        &mut vec![(old.clone(), true), (old.clone(), true)].into(),
        None,
        None,
    )
    .unwrap();
    b.process_records(
        &mut vec![(old, false), (vec![2.into(), "new".into()], true)].into(),
        None,
        None,
    )
    .unwrap();
    b.process_records(&mut Records::default(), None, Some(offset(7)))
        .unwrap();
    // Deliberately bypass PersistentState::drop, including its clean WAL shutdown.
    // This checks process-exit recovery, not disk/controller power-loss guarantees.
    std::process::exit(0);
}
