//! Real process restarts over ReadySet's own persistence: standalone authority recipe, base
//! RocksDB rows, restart markers and declared caches. Each life is a separate child process
//! (the standalone authority keeps its leader in a process-global store, so an in-process
//! "restart" is not a restart). The crash life ends in `abort()`: no destructors, no
//! shutdown. Databend-side windows are covered by the live fault-injection verifier.
use super::*;

const PHASE: &str = "RS_RESTART_PHASE";

fn schema() -> Arc<SourceSchema> {
    Arc::new(
        SourceSchema::from_describe(
            "a",
            "items",
            &[
                vec![json!("id"), json!("BIGINT UNSIGNED"), json!("NO")],
                vec![json!("label"), json!("VARCHAR"), json!("YES")],
            ],
        )
        .unwrap(),
    )
}

async fn start() -> Arc<SharedEngine> {
    SharedEngine::start(
        Path::new(&std::env::var("RS_RESTART_DIR").unwrap()),
        &std::env::var("RS_RESTART_DEPLOYMENT").unwrap(),
        CacheConfig {
            partial: std::env::var("RS_RESTART_PARTIAL").unwrap() == "true",
            ..Default::default()
        },
        vec![schema()],
    )
    .await
    .unwrap()
}

async fn local_rows(engine: &SharedEngine, sql: &str) -> Vec<Vec<Value>> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match engine.query(sql, "a").await.unwrap() {
                ReadOutcome::Matched { mut data, .. } => {
                    data.sort_by_key(|r| serde_json::to_string(r).unwrap());
                    break data;
                }
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("persisted cache must serve locally")
}

async fn insert(engine: &Arc<SharedEngine>, sequence: u64, values: &[(&str, &str)]) -> SharedBatch {
    let s = schema();
    let mut batch = engine.begin_batch("a.items", sequence).await.unwrap();
    batch
        .push(
            values
                .iter()
                .map(|(id, label)| TypedChange {
                    action: Action::Insert,
                    row: s.decode_row(&[json!(id), json!(label)]).unwrap(),
                })
                .collect(),
        )
        .await
        .unwrap();
    batch
}

const SQL: &str = "SELECT id,label FROM a.items WHERE id=7";

fn kept() -> Vec<Vec<Value>> {
    vec![vec![json!("7"), json!("kept")]; 2]
}

/// One life of the restartable engine; only runs as a child of the test below.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "child process of restart_keeps_rows_caches_and_detects_interrupted_batches"]
async fn restart_child() {
    let Ok(phase) = std::env::var(PHASE) else {
        return;
    };
    let engine = start().await;
    match phase.as_str() {
        "first" => {
            // Snapshot-equivalent state at Stream version 10, then one committed batch of
            // duplicate rows (version 11) and one declared cache.
            assert_eq!(engine.decide("a.items", 10).unwrap(), Decision::Create);
            engine.committed("a.items", 10).await.unwrap();
            let mut batch = insert(&engine, 1, &[("7", "kept"), ("7", "kept")]).await;
            batch.mark_applying().await.unwrap();
            assert_eq!(engine.marker_phase("a.items"), Some(Phase::Applying(10)));
            batch.finish().await.unwrap();
            engine.committed("a.items", 11).await.unwrap();
            let created = engine
                .cache_ddl(
                    "CREATE CACHE by_id FROM SELECT id,label FROM a.items WHERE id=?",
                    "a",
                    |_| true,
                )
                .await
                .unwrap();
            assert_eq!(created["ok"], true, "{created}");
            assert_eq!(local_rows(&engine, SQL).await, kept());
        }
        "clean" => {
            // Clean restart: nothing reloaded; rows, cache and marker survive.
            assert_eq!(engine.decide("a.items", 11).unwrap(), Decision::Resume(11));
            assert!(matches!(
                engine.decide("a.items", 12).unwrap(),
                Decision::Rebuild(_)
            ));
            engine.resume("a.items", 11).await.unwrap();
            assert_eq!(local_rows(&engine, SQL).await, kept());
            assert_eq!(engine.listing()["caches"], json!(["by_id"]));
            // Crash mid-batch: a row reaches the base, then the process dies hard.
            let _batch = insert(&engine, 2, &[("8", "partial")]).await;
            assert_eq!(engine.marker_phase("a.items"), Some(Phase::Applying(11)));
            println!("CRASH_POINT_REACHED");
            std::process::abort();
        }
        "after_crash" => {
            // Stream unmoved: batch not committed, only this table is rebuilt.
            assert!(matches!(
                engine.decide("a.items", 11).unwrap(),
                Decision::Rebuild(_)
            ));
            // Stream moved: COMMIT executed (response possibly lost) after all native ACKs.
            assert_eq!(engine.decide("a.items", 12).unwrap(), Decision::Resume(12));
            engine.resume("a.items", 12).await.unwrap();
            assert_eq!(engine.marker_phase("a.items"), Some(Phase::Applied(12)));
            assert_eq!(local_rows(&engine, SQL).await, kept());
        }
        "final" => {
            assert_eq!(engine.decide("a.items", 12).unwrap(), Decision::Resume(12));
        }
        other => panic!("unknown phase {other}"),
    }
    engine.shutdown().await;
    println!("PHASE_OK {phase}");
}

#[test]
fn restart_keeps_rows_caches_and_detects_interrupted_batches() {
    for partial in [false, true] {
        let dir = std::env::temp_dir().join(format!("rs-restart-{}", uuid::Uuid::new_v4()));
        let deployment = format!("restart_{}", uuid::Uuid::new_v4().simple());
        for phase in ["first", "clean", "after_crash", "final"] {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "shared_engine::restart_tests::restart_child",
                    "--ignored",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(PHASE, phase)
                .env("RS_RESTART_DIR", &dir)
                .env("RS_RESTART_DEPLOYMENT", &deployment)
                .env("RS_RESTART_PARTIAL", partial.to_string())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(180);
            while child.try_wait().unwrap().is_none() {
                if std::time::Instant::now() > deadline {
                    child.kill().unwrap();
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            let output = child.wait_with_output().unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if phase == "clean" {
                assert!(
                    stdout.contains("CRASH_POINT_REACHED") && !output.status.success(),
                    "crash life must reach the crash point and die: {stdout}\n{stderr}"
                );
            } else {
                assert!(
                    output.status.success() && stdout.contains(&format!("PHASE_OK {phase}")),
                    "phase {phase} (partial={partial}) failed: {stdout}\n{stderr}"
                );
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
