use super::*;

/// B must finish DURABLY while A still owns an incomplete batch. A timeout here is a
/// regression to whole-batch global serialization, not a performance benchmark.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_native_wal_batches_and_same_source_ordering() {
    for partial in [false, true] {
        let dir = std::env::temp_dir().join(format!("rs-shared-parallel-{}", uuid::Uuid::new_v4()));
        let schemas: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|db| {
                Arc::new(
                    SourceSchema::from_describe(
                        db,
                        "items",
                        &[
                            vec![json!("id"), json!("VARCHAR"), json!("NO")],
                            vec![json!("value"), json!("VARCHAR"), json!("YES")],
                        ],
                    )
                    .unwrap(),
                )
            })
            .collect();
        let engine = SharedEngine::start(
            &dir,
            &format!("parallel_{}", uuid::Uuid::new_v4().simple()),
            CacheConfig {
                partial,
                ..Default::default()
            },
            schemas.clone(),
        )
        .await
        .unwrap();
        assert_eq!(engine.cache_status()["durability"], "native_sync_wal");
        for db in ["a", "b"] {
            assert_eq!(
                engine
                    .cache_ddl(
                        &format!(
                            "CREATE CACHE {db}_id FROM SELECT id,value FROM {db}.items WHERE id=?"
                        ),
                        db,
                        |_| true,
                    )
                    .await
                    .unwrap()["ok"],
                true,
            );
        }
        let ar = schemas[0]
            .decode_row(&[json!("a-key"), json!("unpublished")])
            .unwrap();
        let br = schemas[1]
            .decode_row(&[json!("b-key"), json!("published")])
            .unwrap();
        let aq = "SELECT id,value FROM items WHERE id='a-key'";
        let bq = "SELECT id,value FROM items WHERE id='b-key'";
        let mut a = engine.begin_batch("a.items", 1).await.unwrap();
        a.push(vec![TypedChange {
            action: Action::Insert,
            row: ar,
        }])
        .await
        .unwrap();
        assert!(engine.source_busy("a.items"));
        assert!(matches!(
            engine.query(aq, "a").await.unwrap(),
            ReadOutcome::NotMatched
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut b = engine.begin_batch("b.items", 1).await.unwrap();
            b.push(
                (0..2)
                    .map(|_| TypedChange {
                        action: Action::Insert,
                        row: br.clone(),
                    })
                    .collect(),
            )
            .await
            .unwrap();
            b.finish().await.unwrap();
            // Empty rounds also advance B independently, without hiding its resident keys.
            engine
                .begin_batch("b.items", 2)
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
        })
        .await
        .expect("B must not wait for A's incomplete batch");
        assert_eq!(engine.sources["a.items"].checkpoint.load(Acquire), 0);
        assert_eq!(engine.sources["b.items"].checkpoint.load(Acquire), 2);
        match engine.query(bq, "b").await.unwrap() {
            ReadOutcome::Matched { data, .. } => {
                assert_eq!(data, vec![vec![json!("b-key"), json!("published")]; 2])
            }
            _ => panic!("B must be independently readable"),
        }
        // A same-source waiter must validate AFTER taking Table ownership, not use the
        // offset it saw before the preceding writer finished.
        let b = engine.begin_batch("b.items", 3).await.unwrap();
        let duplicate_engine = Arc::clone(&engine);
        let duplicate =
            tokio::spawn(async move { duplicate_engine.begin_batch("b.items", 3).await });
        tokio::task::yield_now().await;
        assert!(!duplicate.is_finished());
        b.finish().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), duplicate)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(!engine.sources["b.items"].failed.load(Acquire));
        // A queued migration cannot run over A's partial batch. Cancelling before the
        // exclusive guard is acquired has no native side effect and must not poison.
        let migrate_engine = Arc::clone(&engine);
        let migration = tokio::spawn(async move {
            migrate_engine
                .cache_ddl("DROP CACHE a_id", "a", |_| true)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while engine.migrations.try_read().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!migration.is_finished());
        migration.abort();
        assert!(migration.await.unwrap_err().is_cancelled());
        assert!(!engine.is_failed());
        a.finish().await.unwrap();
        assert!(matches!(
            engine.query(aq, "a").await.unwrap(),
            ReadOutcome::Matched { .. }
        ));
        assert_eq!(engine.sources["a.items"].checkpoint.load(Acquire), 1);
        // An abandoned A batch poisons only A; B can still progress its durable offset.
        let mut abandoned = engine.begin_batch("a.items", 2).await.unwrap();
        abandoned
            .push(vec![TypedChange {
                action: Action::Insert,
                row: br,
            }])
            .await
            .unwrap();
        drop(abandoned);
        tokio::time::timeout(Duration::from_secs(5), async {
            engine
                .begin_batch("b.items", 4)
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
        })
        .await
        .unwrap();
        assert!(engine.sources["a.items"].failed.load(Acquire));
        assert!(!engine.sources["b.items"].failed.load(Acquire));
        assert!(!engine.is_failed());
        engine.shutdown().await;
        std::fs::remove_dir_all(dir).unwrap();
    }
}
