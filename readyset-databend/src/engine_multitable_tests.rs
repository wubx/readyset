//! Prove heterogeneous tables use ONE existing ReadySet controller and the SAME native
//! adapter read path. This does not yet claim the HTTP/Stream orchestrator is multi-table.
use super::*;
use crate::native_query::Input;
use crate::source_schema::SourceSchema;

async fn lookup(
    loader: &mut Loader,
    source: &SourceSchema,
    template: &NativeQuery,
    view: &mut View,
    sql: &str,
) -> native_query::LocalResult {
    let query = native_query::compile_for_source(
        native_query::parse_read_select(sql).unwrap(),
        source,
        &source.database,
        Input::Read,
    )
    .unwrap();
    let params = template.read_params(&query).expect("same native shape");
    native_query::read_with(view, Some(&mut loader.handler), &query, &params)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heterogeneous_tables_share_native_controller_full_and_partial() {
    for partial in [false, true] {
        let storage = std::env::temp_dir().join(format!("rs-multitable-{}", uuid::Uuid::new_v4()));
        let mut loader = Engine::loader(EngineConfig {
            generation: format!("multi_{}", uuid::Uuid::new_v4().simple()),
            storage_dir: storage.clone(),
            declaration: None,
            oracle: false,
            cache: CacheConfig {
                partial,
                ..Default::default()
            },
        })
        .await
        .unwrap();
        let a = SourceSchema::from_describe(
            "a",
            "items",
            &[
                vec![json!("id"), json!("VARCHAR"), json!("NO")],
                vec![json!("active"), json!("BOOLEAN"), json!("NO")],
                vec![json!("amount"), json!("DECIMAL(18,2)"), json!("YES")],
            ],
        )
        .unwrap();
        let b = SourceSchema::from_describe(
            "b",
            "items",
            &[
                vec![json!("id"), json!("BIGINT UNSIGNED"), json!("NO")],
                vec![json!("value"), json!("VARCHAR"), json!("YES")],
            ],
        )
        .unwrap();
        loader
            .handle
            .extend_recipe(
                ChangeList::from_strings(vec![a.ddl(), b.ddl()], Dialect::DEFAULT_MYSQL).unwrap(),
            )
            .await
            .unwrap();
        let mut at = loader.handle.table(a.local.as_str()).await.unwrap();
        let mut bt = loader.handle.table(b.local.as_str()).await.unwrap();
        let ar = a
            .decode_row(&[json!("same-key"), json!("true"), json!("12.30")])
            .unwrap();
        let br = b
            .decode_row(&[json!("18446744073709551615"), Value::Null])
            .unwrap();
        at.insert_many([ar.clone(), ar.clone()]).await.unwrap();
        bt.insert_many([br.clone()]).await.unwrap();
        let aq = native_query::compile_for_source(
            native_query::parse_select("SELECT id,active,amount FROM a.items WHERE id=?").unwrap(),
            &a,
            "a",
            Input::Declaration,
        )
        .unwrap();
        let bq = native_query::compile_for_source(
            native_query::parse_select("SELECT id,value FROM b.items WHERE id=?").unwrap(),
            &b,
            "b",
            Input::Declaration,
        )
        .unwrap();
        for (name, query) in [("source_a", &aq), ("source_b", &bq)] {
            loader
                .handle
                .extend_recipe(ChangeList::from_change(
                    Change::create_cache(
                        name,
                        query.statement.clone(),
                        TrxCachePolicy::Never,
                        None,
                        None,
                    ),
                    Dialect::DEFAULT_MYSQL,
                ))
                .await
                .unwrap();
        }
        let mut av = loader.handle.view("source_a").await.unwrap();
        let mut bv = loader.handle.view("source_b").await.unwrap();
        let asql = "SELECT id,active,amount FROM a.items WHERE id='same-key'";
        let bsql = "SELECT id,value FROM b.items WHERE id=18446744073709551615";
        let (rows, schema) = lookup(&mut loader, &a, &aq, &mut av, asql).await;
        assert_eq!(
            rows,
            vec![vec![json!("same-key"), json!("1"), json!("12.30")]; 2]
        );
        assert_eq!(schema[2]["type"], "Nullable(Decimal(18, 2))");
        assert_eq!(
            lookup(&mut loader, &b, &bq, &mut bv, bsql).await.0,
            vec![vec![json!("18446744073709551615"), Value::Null]]
        );
        at.perform_all(vec![TableOperation::DeleteRow { row: ar }])
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if lookup(&mut loader, &a, &aq, &mut av, asql).await.0.len() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // Updating one base never changes the other database's identically named table.
        assert_eq!(lookup(&mut loader, &b, &bq, &mut bv, bsql).await.0.len(), 1);
        loader.shutdown.shutdown().await;
        drop(loader.handle);
        drop(at);
        drop(bt);
        drop(av);
        drop(bv);
        std::fs::remove_dir_all(storage).unwrap();
    }
}
