use super::*;

async fn rows(engine: &SharedEngine, sql: &str, db: &str) -> Vec<Vec<Value>> {
    match engine.query(sql, db).await.unwrap() {
        ReadOutcome::Matched { data, .. } => bag(data),
        _ => panic!("expected local match for {sql}"),
    }
}
fn bag(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_key(|r| serde_json::to_string(r).unwrap());
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_runtime_checkpoints_gates_eviction_and_failure_isolation() {
    for partial in [false, true] {
        let dir = std::env::temp_dir().join(format!("rs-shared-{}", uuid::Uuid::new_v4()));
        let a = Arc::new(
            SourceSchema::from_describe(
                "a",
                "items",
                &[
                    vec![json!("id"), json!("VARCHAR"), json!("NO")],
                    vec![json!("active"), json!("BOOLEAN"), json!("NO")],
                    vec![json!("amount"), json!("DECIMAL(18,2)"), json!("YES")],
                ],
            )
            .unwrap(),
        );
        let b = Arc::new(
            SourceSchema::from_describe(
                "b",
                "items",
                &[
                    vec![json!("id"), json!("BIGINT UNSIGNED"), json!("NO")],
                    vec![json!("value"), json!("VARCHAR"), json!("YES")],
                ],
            )
            .unwrap(),
        );
        let engine = SharedEngine::start(
            &dir,
            &format!("shared_{}", uuid::Uuid::new_v4().simple()),
            CacheConfig {
                partial,
                ..Default::default()
            },
            vec![],
            vec![Arc::clone(&a), Arc::clone(&b)],
        )
        .await
        .unwrap();
        let ar = a
            .decode_row(&[json!("same-key"), json!("1"), json!("12.30")])
            .unwrap();
        let br = b
            .decode_row(&[json!("18446744073709551615"), Value::Null])
            .unwrap();
        for (id, row) in [("a.items", ar.clone()), ("b.items", br.clone())] {
            let mut writer = engine.begin_batch(id, 1).await.unwrap();
            writer
                .push(vec![
                    TypedChange {
                        action: Action::Insert,
                        row: row.clone(),
                    },
                    TypedChange {
                        action: Action::Insert,
                        row,
                    },
                ])
                .await
                .unwrap();
            writer.finish().await.unwrap();
        }
        assert_eq!(
            engine.handle.lock().await.tables().await.unwrap().len(),
            2,
            "no extra native base"
        );
        assert_eq!(engine.cache_status()["native_server_instances"], 1);
        for sql in [
            "CREATE CACHE a_cache FROM SELECT id,active,amount FROM a.items WHERE id=?",
            "CREATE CACHE b_cache FROM SELECT id,value FROM b.items WHERE id=?",
            "CREATE CACHE a_bool FROM SELECT id,active FROM a.items WHERE active=?",
        ] {
            let result = engine.cache_ddl(sql, "default", |_| true).await.unwrap();
            assert_eq!(result["ok"], true, "{result}");
        }
        let rejected = engine
            .cache_ddl(
                "CREATE CACHE unsafe_bool FROM SELECT active FROM a.items WHERE active=?",
                "a",
                |_| true,
            )
            .await
            .unwrap();
        assert_eq!(
            rejected["rejected"], true,
            "no ambiguous small-domain-only fence"
        );
        assert_eq!(engine.routes.read().unwrap().len(), 3);
        let aq = "SELECT id,active,amount FROM items WHERE id='same-key'";
        let bq = "SELECT id,value FROM items WHERE id=18446744073709551615";
        assert_eq!(
            rows(&engine, aq, "a").await,
            vec![vec![json!("same-key"), json!("1"), json!("12.30")]; 2]
        );
        assert_eq!(
            rows(&engine, bq, "b").await,
            vec![vec![json!("18446744073709551615"), Value::Null]; 2]
        );
        assert!(matches!(
            engine.query(bq, "a").await.unwrap(),
            ReadOutcome::NotMatched
        ));
        assert_eq!(
            engine.read_source(
                "SELECT a.id FROM a.items a JOIN b.items b ON a.id=b.id WHERE a.id=1",
                "a"
            ),
            None
        );
        // Divergent checkpoints: native sync-WAL ACKs certify B independently of A=1.
        let mut batch = engine.begin_batch("b.items", 2).await.unwrap();
        let mut updated = br.clone();
        updated[1] = DfValue::from("changed");
        batch
            .push(vec![
                TypedChange {
                    action: Action::Delete,
                    row: br.clone(),
                },
                TypedChange {
                    action: Action::Insert,
                    row: updated,
                },
            ])
            .await
            .unwrap();
        assert!(engine.source_busy("b.items"));
        assert_eq!(
            rows(&engine, aq, "a").await.len(),
            2,
            "B's gate does not exclude A"
        );
        assert!(matches!(
            engine.query(bq, "b").await.unwrap(),
            ReadOutcome::NotMatched
        ));
        batch.finish().await.unwrap();
        assert_eq!(engine.src("a.items").checkpoint.load(Acquire), 1);
        assert_eq!(engine.src("b.items").checkpoint.load(Acquire), 2);
        let expected = bag(vec![
            vec![json!("18446744073709551615"), Value::Null],
            vec![json!("18446744073709551615"), json!("changed")],
        ]);
        assert_eq!(rows(&engine, bq, "b").await, expected);
        // Empty captures advance durable offsets without disrupting resident readers.
        let empty = engine.begin_batch("b.items", 3).await.unwrap();
        assert!(!engine.source_busy("b.items"));
        assert_eq!(rows(&engine, bq, "b").await, expected);
        empty.finish().await.unwrap();
        if partial {
            let before = engine.cache_status()["readers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["eviction_epoch"].as_u64().unwrap())
                .sum::<u64>();
            for _ in 0..20 {
                engine.handle.lock().await.evict_single(None).await.unwrap();
                assert_eq!(rows(&engine, aq, "a").await.len(), 2);
                assert_eq!(rows(&engine, bq, "b").await, expected);
            }
            let after = engine.cache_status()["readers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["eviction_epoch"].as_u64().unwrap())
                .sum::<u64>();
            assert!(
                after > before,
                "real native eviction, not a fabricated miss"
            );
            assert!(engine.cache_status()["miss_keys"].as_u64().unwrap() > 0);
        }
        // Abandon a partially-applied A batch; B's native state and reads must survive.
        let mut abandoned = engine.begin_batch("a.items", 2).await.unwrap();
        abandoned
            .push(vec![TypedChange {
                action: Action::Insert,
                row: ar,
            }])
            .await
            .unwrap();
        drop(abandoned);
        assert!(engine.src("a.items").failed.load(Acquire));
        assert!(!engine.src("b.items").failed.load(Acquire));
        assert!(matches!(
            engine.query(aq, "a").await.unwrap(),
            ReadOutcome::NotMatched
        ));
        assert_eq!(rows(&engine, bq, "b").await, expected);
        engine
            .begin_batch("b.items", 4)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        assert_eq!(rows(&engine, bq, "b").await, expected);
        engine.shutdown().await;
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_zero_wait_replay_and_cancelled_migration_fail_closed() {
    let dir = std::env::temp_dir().join(format!("rs-shared-timeout-{}", uuid::Uuid::new_v4()));
    let schema = Arc::new(
        SourceSchema::from_describe(
            "a",
            "items",
            &[
                vec![json!("id"), json!("BIGINT UNSIGNED"), json!("NO")],
                vec![json!("label"), json!("VARCHAR"), json!("YES")],
            ],
        )
        .unwrap(),
    );
    let engine = SharedEngine::start(
        &dir,
        &format!("shared_{}", uuid::Uuid::new_v4().simple()),
        CacheConfig {
            partial: true,
            upquery_timeout: Duration::ZERO,
            ..Default::default()
        },
        vec![],
        vec![Arc::clone(&schema)],
    )
    .await
    .unwrap();
    let mut batch = engine.begin_batch("a.items", 1).await.unwrap();
    batch
        .push(vec![TypedChange {
            action: Action::Insert,
            row: schema.decode_row(&[json!("7"), json!("replayed")]).unwrap(),
        }])
        .await
        .unwrap();
    batch.finish().await.unwrap();
    assert_eq!(
        engine
            .cache_ddl(
                "CREATE CACHE by_id FROM SELECT id,label FROM a.items WHERE id=?",
                "a",
                |_| true
            )
            .await
            .unwrap()["ok"],
        true
    );
    let sql = "SELECT id,label FROM a.items WHERE id=7";
    assert!(matches!(
        engine.query(sql, "a").await.unwrap(),
        ReadOutcome::NotMatched
    ));
    assert!(engine.cache_status()["replay_timeouts"].as_u64().unwrap() > 0);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match engine.query(sql, "a").await.unwrap() {
                ReadOutcome::Matched { data, .. } => {
                    assert_eq!(data, vec![vec![json!("7"), json!("replayed")]]);
                    break;
                }
                ReadOutcome::NotMatched => tokio::time::sleep(Duration::from_millis(10)).await,
                _ => panic!("unexpected result-limit miss"),
            }
        }
    })
    .await
    .unwrap();
    assert!(!engine.failed.load(Acquire));
    assert!(!engine.src("a.items").failed.load(Acquire));
    // Cancel a cache migration while it owns the coordinator. A dropped request must
    // fail closed, not silently leave an unknown native graph behind live HTTP routes.
    let handle = engine.handle.lock().await;
    let task_engine = Arc::clone(&engine);
    let migration = tokio::spawn(async move {
        task_engine
            .cache_ddl("DROP CACHE by_id", "a", |_| true)
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if engine.migrations.try_read().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    migration.abort();
    assert!(migration.await.unwrap_err().is_cancelled());
    drop(handle);
    assert!(engine.is_failed());
    assert_eq!(engine.cache_status()["failed"], true);
    assert!(engine.query(sql, "a").await.is_err());
    engine.shutdown().await;
    std::fs::remove_dir_all(dir).unwrap();
}
