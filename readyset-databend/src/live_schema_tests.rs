//! Explicitly opt-in real Databend test; creates only new readyset_gateway_* databases.
//! It tests the new primitives, NOT yet the HTTP multi-table configuration/orchestrator.
use super::*;
use crate::native_query::Input;
use crate::replicator::{Names, Replicator, parse_typed_changes};
use crate::upstream::{DatabendClient, basic_auth, http_client};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires authorized Databend endpoint; creates retained test databases"]
async fn heterogeneous_source_streams_use_one_controller() {
    let endpoint = std::env::var("DATABEND_TEST_ENDPOINT").expect("explicit endpoint required");
    let auth = basic_auth(
        "root",
        &std::env::var("DATABEND_PASSWORD").unwrap_or_default(),
    );
    let client = |label| DatabendClient::new(http_client().unwrap(), &endpoint, &auth, label, None);
    let mut admin = client("test-admin");
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let generation = format!("multi_{nonce}");
    let dbs = [
        format!("readyset_gateway_multi_{}_a", &nonce[..12]),
        format!("readyset_gateway_multi_{}_b", &nonce[..12]),
    ];
    eprintln!("Retaining test databases: {dbs:?}");
    for db in &dbs {
        admin.sql(&format!("CREATE DATABASE {db}")).await.unwrap();
    }
    admin.sql(&format!("CREATE TABLE {}.items (id STRING NOT NULL, active BOOLEAN NOT NULL, amount DECIMAL(18,2) NULL)", dbs[0])).await.unwrap();
    admin
        .sql(&format!(
            "INSERT INTO {}.items VALUES ('same-key', true, 12.30), ('same-key', true, 12.30)",
            dbs[0]
        ))
        .await
        .unwrap();
    admin
        .sql(&format!(
            "CREATE TABLE {}.items (id BIGINT UNSIGNED NOT NULL, value STRING NULL)",
            dbs[1]
        ))
        .await
        .unwrap();
    admin
        .sql(&format!(
            "INSERT INTO {}.items VALUES (18446744073709551615, NULL)",
            dbs[1]
        ))
        .await
        .unwrap();
    let mut sources = Vec::new();
    for db in &dbs {
        let names = Names::new(db, "items", &nonce[..12], &generation);
        let mut source = Replicator::new(names, client("admin"), client("capture"), false);
        source.prepare_schema().await.unwrap();
        sources.push(source);
    }
    let storage = std::env::temp_dir().join(format!("rs-live-multi-{nonce}"));
    let mut loader = Engine::loader(EngineConfig {
        generation: generation.clone(),
        storage_dir: storage.clone(),
        declaration: None,
        oracle: false,
        cache: CacheConfig {
            partial: true,
            memory_limit_bytes: Some(2 * 1024 * 1024 * 1024),
            ..Default::default()
        },
    })
    .await
    .unwrap();
    let schemas: Vec<_> = sources.iter().map(|s| s.schema.clone().unwrap()).collect();
    loader
        .handle
        .extend_recipe(
            ChangeList::from_strings(
                schemas.iter().map(|s| s.ddl()).collect::<Vec<_>>(),
                Dialect::DEFAULT_MYSQL,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let offset = |sequence| {
        ReplicationOffset::Databend(DatabendPosition::new(&generation, sequence).unwrap())
    };
    loader
        .table
        .set_replication_offset(offset(0))
        .await
        .unwrap();
    let mut tables = Vec::new();
    for source in &mut sources {
        let mut table = loader
            .handle
            .table(source.schema.as_ref().unwrap().local.as_str())
            .await
            .unwrap();
        source.snapshot_into(&mut table).await.unwrap();
        table.set_replication_offset(offset(0)).await.unwrap();
        tables.push(table);
    }
    assert_eq!(sources.iter().map(|s| s.rows).collect::<Vec<_>>(), [2, 1]);
    for i in 0..2 {
        let projection = if i == 0 {
            "id,active,amount"
        } else {
            "id,value"
        };
        let sql = format!("SELECT {projection} FROM {}.items WHERE id=?", dbs[i]);
        let template = native_query::compile_for_source(
            native_query::parse_select(&sql).unwrap(),
            &schemas[i],
            &dbs[i],
            Input::Declaration,
        )
        .unwrap();
        let name = format!("live_{i}");
        loader
            .handle
            .extend_recipe(ChangeList::from_change(
                Change::create_cache(
                    name.clone(),
                    template.statement.clone(),
                    TrxCachePolicy::Never,
                    None,
                    None,
                ),
                Dialect::DEFAULT_MYSQL,
            ))
            .await
            .unwrap();
        let id = if i == 0 {
            "'same-key'"
        } else {
            "18446744073709551615"
        };
        let sql = format!("SELECT {projection} FROM {}.items WHERE id={id}", dbs[i]);
        let query = native_query::compile_for_source(
            native_query::parse_read_select(&sql).unwrap(),
            &schemas[i],
            &dbs[i],
            Input::Read,
        )
        .unwrap();
        let params = template.read_params(&query).unwrap();
        let mut view = loader.handle.view(name).await.unwrap();
        let actual = native_query::read_with(&mut view, Some(&mut loader.handler), &query, &params)
            .await
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(actual, admin.sql(&sql).await.unwrap());
    }
    // Complete the snapshot's pinned transaction before testing ordinary rollback.
    // The first consume shares the source/Stream versions fixed during snapshot.
    for i in 0..2 {
        let mut pages = sources[i].begin_consume().await.unwrap();
        assert!(pages.next().await.unwrap().is_none());
        pages.close().await.unwrap();
        tables[i].set_replication_offset(offset(1)).await.unwrap();
        sources[i].commit_consumed(0, 0).await.unwrap();
    }
    admin
        .sql(&format!(
            "INSERT INTO {}.items VALUES ('new-key', false, NULL)",
            dbs[0]
        ))
        .await
        .unwrap();
    admin
        .sql(&format!(
            "UPDATE {}.items SET amount=13.40 WHERE id='same-key'",
            dbs[0]
        ))
        .await
        .unwrap();
    admin
        .sql(&format!(
            "INSERT INTO {}.items VALUES (7, 'independent')",
            dbs[1]
        ))
        .await
        .unwrap();
    // A consuming read followed by ROLLBACK must advance neither source's Stream.
    let schema = schemas[0].clone();
    let mut pages = sources[0].begin_consume().await.unwrap();
    let mut rolled = 0;
    while let Some(page) = pages.next().await.unwrap() {
        rolled += parse_typed_changes(&schema, &page).unwrap().len();
    }
    pages.close().await.unwrap();
    assert_eq!(rolled, 5);
    let _ = sources[0]
        .abort_capture(anyhow::anyhow!("intentional rollback test"))
        .await;
    for i in 0..2 {
        let mut pages = sources[i].begin_consume().await.unwrap();
        let mut changes = Vec::new();
        while let Some(page) = pages.next().await.unwrap() {
            changes.extend(parse_typed_changes(&schemas[i], &page).unwrap());
        }
        pages.close().await.unwrap();
        assert_eq!(
            changes.len(),
            if i == 0 { 5 } else { 1 },
            "rollback/other-table capture must not consume these changes"
        );
        let deletes = changes
            .iter()
            .filter(|c| c.action == Action::Delete)
            .count() as u64;
        let inserts = changes.len() as u64 - deletes;
        let operations = changes
            .iter()
            .filter(|c| c.action == Action::Delete)
            .map(|c| TableOperation::DeleteRow { row: c.row.clone() })
            .chain(
                changes
                    .iter()
                    .filter(|c| c.action == Action::Insert)
                    .map(|c| TableOperation::Insert(c.row.clone())),
            );
        tables[i].perform_all(operations).await.unwrap();
        tables[i].set_replication_offset(offset(2)).await.unwrap();
        Sink::new(generation.clone(), schemas[i].local.clone())
            .unwrap()
            .wait_persisted(&mut loader.handle, 2)
            .await
            .unwrap();
        sources[i].commit_consumed(inserts, deletes).await.unwrap();
        assert_eq!(sources[i].sequence, 2);
    }
    for i in 0..2 {
        let projection = if i == 0 {
            "id,active,amount"
        } else {
            "id,value"
        };
        let sql = format!("SELECT {projection} FROM {}.items WHERE id=?", dbs[i]);
        let template = native_query::compile_for_source(
            native_query::parse_select(&sql).unwrap(),
            &schemas[i],
            &dbs[i],
            Input::Declaration,
        )
        .unwrap();
        let sql = format!(
            "SELECT {projection} FROM {}.items WHERE id={}",
            dbs[i],
            if i == 0 { "'new-key'" } else { "7" }
        );
        let query = native_query::compile_for_source(
            native_query::parse_read_select(&sql).unwrap(),
            &schemas[i],
            &dbs[i],
            Input::Read,
        )
        .unwrap();
        let params = template.read_params(&query).unwrap();
        let mut view = loader.handle.view(format!("live_{i}")).await.unwrap();
        let actual = native_query::read_with(&mut view, Some(&mut loader.handler), &query, &params)
            .await
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(actual, admin.sql(&sql).await.unwrap());
        let mut pages = sources[i].begin_consume().await.unwrap();
        while let Some(page) = pages.next().await.unwrap() {
            assert!(
                page.is_empty(),
                "committed change must not be delivered again"
            );
        }
        pages.close().await.unwrap();
        tables[i].set_replication_offset(offset(3)).await.unwrap();
        Sink::new(generation.clone(), schemas[i].local.clone())
            .unwrap()
            .wait_persisted(&mut loader.handle, 3)
            .await
            .unwrap();
        sources[i].commit_consumed(0, 0).await.unwrap();
    }
    let sql = format!(
        "SELECT id,active,amount FROM {}.items WHERE id='same-key'",
        dbs[0]
    );
    let template_sql = format!("SELECT id,active,amount FROM {}.items WHERE id=?", dbs[0]);
    let template = native_query::compile_for_source(
        native_query::parse_select(&template_sql).unwrap(),
        &schemas[0],
        &dbs[0],
        Input::Declaration,
    )
    .unwrap();
    let query = native_query::compile_for_source(
        native_query::parse_read_select(&sql).unwrap(),
        &schemas[0],
        &dbs[0],
        Input::Read,
    )
    .unwrap();
    let params = template.read_params(&query).unwrap();
    let mut view = loader.handle.view("live_0").await.unwrap();
    let updated = native_query::read_with(&mut view, Some(&mut loader.handler), &query, &params)
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(updated, admin.sql(&sql).await.unwrap());
    assert_eq!(updated.len(), 2, "identical updated rows must not collapse");
    for db in &dbs {
        let streams = admin
            .sql(&format!(
                "SELECT count(*) FROM system.streams WHERE database='{db}'"
            ))
            .await
            .unwrap();
        assert_eq!(streams[0][0], "1");
    }
    admin
        .sql(&format!(
            "ALTER TABLE {}.items ADD COLUMN extra STRING NULL",
            dbs[0]
        ))
        .await
        .unwrap();
    assert!(
        sources[0].begin_consume().await.is_err(),
        "observed schema drift must stop this source before BEGIN"
    );
    sources[1].check_schema().await.unwrap();
    assert_eq!(
        sources[1].sequence, 3,
        "other source's checkpoint must remain independent"
    );
    eprintln!(
        "PASS schema discovery, snapshot/native reads, UPDATE bags, rollback, independent commit, schema drift detection, one Stream/table"
    );
    loader.shutdown.shutdown().await;
    drop(loader.handle);
    drop(tables);
    std::fs::remove_dir_all(storage).unwrap();
}
