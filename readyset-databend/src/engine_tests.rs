//! Remote-only integration tests for the Databend partial-reader integration.
use super::*;
use crate::declaration::QueryDeclaration;

fn config(timeout: Duration) -> EngineConfig {
    EngineConfig {
        generation: format!("partial_{}", uuid::Uuid::new_v4().simple()),
        storage_dir: std::env::temp_dir().join(format!("rs-partial-{}", uuid::Uuid::new_v4())),
        declaration: Some(Declaration {
            database: "poc".into(),
            table: "source_rows".into(),
            queries: vec![
                QueryDeclaration {
                    name: "by_id".into(),
                    sql: "SELECT id,payload,score,note FROM poc.source_rows WHERE id=?".into(),
                },
                QueryDeclaration {
                    name: "by_score".into(),
                    sql: "SELECT id,payload FROM poc.source_rows WHERE score IN (?,?)".into(),
                },
                QueryDeclaration {
                    name: "by_range".into(),
                    sql: "SELECT id,payload FROM poc.source_rows WHERE score>=? AND score<?".into(),
                },
            ],
        }),
        oracle: false,
        cache: CacheConfig {
            partial: true,
            memory_limit_bytes: Some(2 * 1024 * 1024 * 1024),
            upquery_timeout: timeout,
        },
    }
}
async fn start_engine(config: EngineConfig, initial: Vec<SourceRow>) -> Engine {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();
    eprintln!("partial test: starting loader");
    let mut loader = tokio::time::timeout(Duration::from_secs(30), Engine::loader(config))
        .await
        .expect("loader startup timed out")
        .unwrap();
    eprintln!("partial test: loading {} rows", initial.len());
    tokio::time::timeout(Duration::from_secs(30), loader.load(&initial))
        .await
        .expect("snapshot write timed out")
        .unwrap();
    eprintln!("partial test: finishing snapshot and fencing");
    tokio::time::timeout(Duration::from_secs(60), loader.finish(Some(initial)))
        .await
        .expect("snapshot reader/fence timed out")
        .unwrap()
}

fn row(id: i64) -> SourceRow {
    SourceRow {
        id,
        payload: format!("p-{id}"),
        score: id % 100,
        note: None,
    }
}
async fn rows(engine: &Engine, sql: &str) -> Vec<Vec<Value>> {
    match engine.query(sql, "poc").await.unwrap() {
        ReadOutcome::Matched { data, .. } => data,
        _ => panic!("expected local read: {sql}"),
    }
}

#[test]
fn memory_limit_requires_evictable_readers() {
    let mut c = CacheConfig::default();
    assert!(c.validate(false).is_ok());
    c.memory_limit_bytes = Some(1);
    assert!(c.validate(false).is_err());
    c.partial = true;
    assert!(c.validate(false).is_ok());
    assert!(c.validate(true).is_err());
    c.memory_limit_bytes = Some(0);
    assert!(c.validate(false).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_cold_hot_replay_duplicates_and_eviction() {
    let config = config(Duration::from_secs(5));
    let storage = config.storage_dir.clone();
    let mut initial: Vec<_> = (0..20000).map(row).collect();
    initial.push(row(7)); // Identical duplicate must survive cache replay and a one-copy delete.
    let engine = Arc::new(start_engine(config, initial).await);
    let info = engine.materializations().await.unwrap();
    assert!(
        info.as_array()
            .unwrap()
            .iter()
            .any(|m| m["partial"] == true)
    );
    for r in engine.cache_status()["readers"].as_array().unwrap() {
        assert!(
            r["key_slots"].as_u64().unwrap() < 10,
            "unexpected full reader: {r}"
        );
    }
    let sql = "SELECT id,payload,score,note FROM source_rows WHERE id=7";
    assert_eq!(rows(&engine, sql).await.len(), 2);
    let misses = engine.cache_status()["miss_keys"].as_u64().unwrap();
    assert!(misses > 0);
    assert_eq!(rows(&engine, sql).await.len(), 2);
    assert_eq!(engine.cache_status()["miss_keys"].as_u64().unwrap(), misses);
    assert_eq!(
        rows(
            &engine,
            "SELECT id,payload FROM source_rows WHERE score IN (7,999)"
        )
        .await
        .len(),
        201
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id,payload FROM source_rows WHERE score>=7 AND score<8"
        )
        .await
        .len(),
        201
    );

    let mut writer = engine.begin_batch(1, false).await.unwrap();
    writer
        .push(vec![SourceChange {
            action: Action::Delete,
            row_id: "x".into(),
            is_update: false,
            row: row(7),
        }])
        .await
        .unwrap();
    writer.finish().await.unwrap();
    assert_eq!(rows(&engine, sql).await.len(), 1);
    assert_eq!(
        rows(
            &engine,
            "SELECT id,payload FROM source_rows WHERE score IN (7,999)"
        )
        .await
        .len(),
        200
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id,payload FROM source_rows WHERE score>=7 AND score<8"
        )
        .await
        .len(),
        200
    );

    // Residency telemetry must not ask the interval tree to inspect a reversed range.
    assert!(
        rows(
            &engine,
            "SELECT id,payload FROM source_rows WHERE score>=9 AND score<7"
        )
        .await
        .is_empty()
    );
    assert!(
        rows(
            &engine,
            "SELECT id,payload FROM source_rows WHERE score>=7 AND score<7"
        )
        .await
        .is_empty()
    );

    // Exercise the native eviction API (no hand-written cache invalidation).
    // Pick and replay keys until one of the populated paths has actually evicted state.
    let mut evicted = false;
    for _ in 0..50 {
        let result = engine
            .core
            .lock()
            .await
            .handle
            .evict_single(None)
            .await
            .unwrap();
        if result.is_some() {
            evicted = true;
            break;
        }
    }
    assert!(evicted, "test failed to exercise a native eviction");
    for _ in 0..5 {
        assert_eq!(rows(&engine, sql).await.len(), 1);
        assert_eq!(
            rows(
                &engine,
                "SELECT id,payload FROM source_rows WHERE score IN (7,999)"
            )
            .await
            .len(),
            200
        );
        assert_eq!(
            rows(
                &engine,
                "SELECT id,payload FROM source_rows WHERE score>=7 AND score<8"
            )
            .await
            .len(),
            200
        );
    }
    // Eviction is independent of the application's batch gate. Race native evictions with
    // a new batch/fence using a separate controller client, then verify hot and cold keys.
    let address = engine.core.lock().await.handle.get_address().clone();
    let evictor = tokio::spawn(async move {
        let mut controller = ReadySetHandle::make_raw(address, Some(Duration::from_secs(10)), None);
        for _ in 0..80 {
            controller.evict_single(None).await.unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });
    let mut writer = engine.begin_batch(2, false).await.unwrap();
    let mut changed = row(7);
    changed.score = 999;
    writer
        .push(vec![
            SourceChange {
                action: Action::Delete,
                row_id: "x".into(),
                is_update: true,
                row: row(7),
            },
            SourceChange {
                action: Action::Insert,
                row_id: "x".into(),
                is_update: true,
                row: changed,
            },
        ])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), writer.finish())
        .await
        .unwrap()
        .unwrap();
    evictor.await.unwrap();
    assert_eq!(rows(&engine, sql).await[0][2], "999");
    assert_eq!(
        rows(
            &engine,
            "SELECT id,payload FROM source_rows WHERE score IN (7,999)"
        )
        .await
        .len(),
        200
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id,payload FROM source_rows WHERE score>=7 AND score<8"
        )
        .await
        .len(),
        199
    );
    engine.check_alive().unwrap();
    engine.shutdown().await;
    std::fs::remove_dir_all(storage).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_wait_cold_miss_is_not_engine_failure() {
    let config = config(Duration::ZERO);
    let storage = config.storage_dir.clone();
    let engine = start_engine(config, (0..100).map(row).collect()).await;
    // Zero wait triggers a native background replay, immediately falling through on a miss.
    let sql = "SELECT id,payload,score,note FROM source_rows WHERE id=77";
    assert!(matches!(
        engine.query(sql, "poc").await.unwrap(),
        ReadOutcome::NotMatched
    ));
    assert_eq!(engine.cache_status()["replay_timeouts"], 1);
    engine.check_alive().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match engine.query(sql, "poc").await.unwrap() {
            ReadOutcome::Matched { data, .. } => {
                assert_eq!(data.len(), 1);
                break;
            }
            _ => {
                assert!(Instant::now() < deadline, "background replay never filled");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    engine.shutdown().await;
    std::fs::remove_dir_all(storage).unwrap();
}
