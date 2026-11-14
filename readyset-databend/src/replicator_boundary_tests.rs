//! Adapter protocol guards; real Stream repeatable-read/CAS tests are opt-in live tests.
use super::*;
use crate::upstream::{basic_auth, http_client};
use axum::{Json, Router, extract::State, routing::post};
use serde_json::json;
use std::sync::Mutex;

#[derive(Default)]
struct Mock {
    sql: Mutex<Vec<String>>,
    fail_commit: bool,
}
async fn query(State(mock): State<Arc<Mock>>, Json(body): Json<Value>) -> Json<Value> {
    let sql = body["sql"].as_str().unwrap();
    mock.sql.lock().unwrap().push(sql.into());
    let failed = sql == "COMMIT" && mock.fail_commit;
    Json(
        json!({"id":"mock", "state":if failed {"Failed"} else {"Succeeded"},
        "error":if failed {json!({"code":2734,"message":"StreamVersionMismatched"})} else {Value::Null},
        "data":[], "next_uri":null,
        "session":{"txn_state":if matches!(sql,"COMMIT"|"ROLLBACK") {"AutoCommit"} else {"Active"}}}),
    )
}
async fn fixture(fail_commit: bool) -> (Replicator, Arc<Mock>, tokio::task::JoinHandle<()>) {
    let mock = Arc::new(Mock {
        fail_commit,
        ..Default::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/v1/query", post(query))
        .fallback(|| async { Json(json!({})) })
        .with_state(Arc::clone(&mock));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = || {
        DatabendClient::new(
            http_client().unwrap(),
            &endpoint,
            &basic_auth("root", ""),
            "test",
            None,
        )
    };
    let capture = Replicator::new(
        Names::new("a", "items", "test", "generation"),
        client(),
        client(),
        false,
    );
    (capture, mock, server)
}

#[tokio::test]
async fn bootstrap_consume_reuses_transaction_and_keeps_original_freshness_start() {
    let (mut capture, mock, server) = fixture(false).await;
    capture.begin_snapshot().await.unwrap();
    let started = capture.bootstrap_started().unwrap();
    let pages = capture.begin_consume().await.unwrap();
    let mut pages = pages;
    assert!(pages.next().await.unwrap().is_none());
    pages.close().await.unwrap();
    assert_eq!(capture.bootstrap_started(), Some(started));
    capture.commit_consumed(0, 0).await.unwrap();
    assert!(capture.bootstrap_started().is_none());
    assert_eq!(capture.sequence, 1);
    assert_eq!(
        mock.sql
            .lock()
            .unwrap()
            .iter()
            .filter(|s| *s == "BEGIN")
            .count(),
        1
    );
    let mut pages = capture.begin_consume().await.unwrap();
    assert!(pages.next().await.unwrap().is_none());
    pages.close().await.unwrap();
    capture.commit_consumed(0, 0).await.unwrap();
    assert_eq!(
        mock.sql
            .lock()
            .unwrap()
            .iter()
            .filter(|s| *s == "BEGIN")
            .count(),
        2
    );
    server.abort();
}

#[tokio::test]
async fn expired_bootstrap_never_silently_opens_new_transaction() {
    let (mut capture, mock, server) = fixture(false).await;
    capture.begin_snapshot().await.unwrap();
    capture
        .capture
        .session
        .insert("txn_state".into(), json!("AutoCommit"));
    assert!(capture.begin_consume().await.is_err());
    assert_eq!(*mock.sql.lock().unwrap(), vec!["BEGIN"]);
    server.abort();
}

#[tokio::test]
async fn aborted_bootstrap_requires_rebuild_not_a_fresh_consume() {
    let (mut capture, mock, server) = fixture(false).await;
    capture.begin_snapshot().await.unwrap();
    let _ = capture
        .abort_capture(anyhow::anyhow!("injected snapshot failure"))
        .await;
    assert!(capture.begin_consume().await.is_err());
    assert!(capture.begin_snapshot().await.is_err());
    assert_eq!(*mock.sql.lock().unwrap(), vec!["BEGIN", "ROLLBACK"]);
    server.abort();
}

#[tokio::test]
async fn commit_conflict_stops_capture_without_retrying_transaction_or_sql() {
    let (mut capture, mock, server) = fixture(true).await;
    capture.begin_snapshot().await.unwrap();
    let mut pages = capture.begin_consume().await.unwrap();
    assert!(pages.next().await.unwrap().is_none());
    pages.close().await.unwrap();
    let err = capture.commit_consumed(0, 0).await.unwrap_err();
    assert!(format!("{err:#}").contains("2734"));
    assert_eq!(capture.sequence, 0);
    assert!(capture.commit_consumed(0, 0).await.is_err());
    let _ = capture.abort_capture(err).await;
    assert!(capture.begin_consume().await.is_err());
    let sql = mock.sql.lock().unwrap();
    assert_eq!(sql.iter().filter(|s| *s == "BEGIN").count(), 1);
    assert_eq!(sql.iter().filter(|s| *s == "COMMIT").count(), 1);
    server.abort();
}
