use super::*;

#[test]
fn finalized_ids_are_remembered_and_bounded() {
    let mut f = Finalized::default();
    f.insert("client-a");
    f.insert("client-a");
    assert!(f.contains("client-a"));
    assert!(!f.contains("client-b"));
    assert_eq!(
        f.order.len(),
        1,
        "re-finalizing an ID must not duplicate it"
    );
    for i in 0..MAX_FINALIZED + 10 {
        f.insert(&format!("id-{i}"));
    }
    assert_eq!(f.ids.len(), MAX_FINALIZED);
    assert_eq!(f.order.len(), MAX_FINALIZED);
    assert!(!f.contains("client-a"), "oldest IDs are evicted first");
    assert!(!f.contains("id-0"));
    assert!(f.contains(&format!("id-{}", MAX_FINALIZED + 9)));
}

#[test]
fn finalized_ids_expire() {
    let mut f = Finalized::default();
    f.insert("old");
    f.prune(Instant::now() + LOCAL_RESULT_TTL + Duration::from_millis(1));
    assert!(f.ids.is_empty() && f.order.is_empty());
}

#[test]
fn route_state_reports_first_unavailable_reason() {
    let now = Instant::now();
    let max = Duration::from_secs(10);
    let mut st = RouteState {
        ready: true,
        last_sync: Some(now),
        ..RouteState::default()
    };
    assert_eq!(st.unavailable(now, max), None);
    assert!(st.available(now, max));
    assert_eq!(
        st.unavailable(now + Duration::from_secs(11), max),
        Some("stale")
    );
    st.ready = false;
    assert_eq!(st.unavailable(now, max), Some("not_ready"));
    st.not_ready = Some("capture_slow");
    assert_eq!(st.unavailable(now, max), Some("capture_slow"));
    st.active_forwards = 1;
    assert_eq!(st.unavailable(now, max), Some("active_forward"));
    st.inflight.insert("q".into());
    assert_eq!(st.unavailable(now, max), Some("upstream_inflight"));
    st.uncertain = true;
    assert_eq!(st.unavailable(now, max), Some("uncertain_upstream"));
    st.failed = Some("x".into());
    assert_eq!(st.unavailable(now, max), Some("table_failed"));
}

/// The session object databend-jdbc 0.4.8 sends (captured from the driver), with its
/// driver-local `autoCommit` flag: local reads stay allowed in either autoCommit mode, and
/// only an active transaction (explicit BEGIN) or other unsafe state forwards.
#[test]
fn jdbc_session_with_auto_commit_flag_is_safe() {
    let body = |session: Value| -> Map<String, Value> {
        json!({"sql": "SELECT 1", "session": session})
            .as_object()
            .unwrap()
            .clone()
    };
    let jdbc = |auto: Value, txn: &str| {
        json!({"autoCommit": auto, "catalog": "default", "database": "db", "need_keep_alive": false,
               "need_sticky": false, "role": "account_admin", "settings": {}, "txn_state": txn,
               "internal": "{\"last_node_id\":\"n1\",\"last_query_ids\":[\"q1\"]}"})
    };
    let role = Some("account_admin");
    // First statement on a connection: txn_state null, no role/catalog/internal yet, and the
    // driver's pagination hints.
    let first: Map<String, Value> = json!({"sql": "SELECT 1",
        "pagination": {"wait_time_secs": 10, "max_rows_in_buffer": 5_000_000, "max_rows_per_page": 10_000},
        "session": {"database": "db", "settings": {}, "txn_state": null, "need_sticky": false,
                    "need_keep_alive": false, "autoCommit": false}})
    .as_object()
    .unwrap()
    .clone();
    assert!(safe_session(&first, role));
    let mut odd = first.clone();
    odd["pagination"]["max_rows_per_page"] = json!(-1);
    assert!(!safe_session(&odd, role));
    let mut odd = first.clone();
    odd["pagination"]["wait_time_secs"] = json!(100_000);
    assert!(!safe_session(&odd, role));
    assert!(safe_session(&body(jdbc(json!(true), "AutoCommit")), role));
    assert!(safe_session(&body(jdbc(json!(false), "AutoCommit")), role));
    assert!(!safe_session(&body(jdbc(json!(false), "Active")), role));
    assert!(!safe_session(
        &body(jdbc(json!("false"), "AutoCommit")),
        role
    ));
    let mut temp_tables = jdbc(json!(true), "AutoCommit");
    temp_tables["need_keep_alive"] = json!(true);
    assert!(!safe_session(&body(temp_tables), role));
}

#[test]
fn reply_keeps_every_set_cookie() {
    let cookie = |v: &'static str| (header::SET_COOKIE, HeaderValue::from_static(v));
    let r = reply(
        StatusCode::OK,
        "{}",
        vec![cookie("session_id=a"), cookie("last_refresh_time=1")],
    );
    let got: Vec<_> = r.headers().get_all(header::SET_COOKIE).iter().collect();
    assert_eq!(got, ["session_id=a", "last_refresh_time=1"]);
}

fn stored(rows: usize, bytes: usize) -> Stored {
    Stored {
        base: Map::new(),
        rows: vec![json!([1]); rows],
        page_size: 10,
        bytes,
    }
}

#[test]
fn last_page_matches_next_uri() {
    assert!(last_page(&stored(0, 1), 0));
    assert!(last_page(&stored(10, 1), 0));
    assert!(!last_page(&stored(11, 1), 0));
    assert!(last_page(&stored(11, 1), 1));
    let s = stored(11, 1);
    assert!(local_page(&s, "/v1/query/q", 0)["next_uri"] == "/v1/query/q/page/1");
    assert!(local_page(&s, "/v1/query/q", 1)["next_uri"] == "/v1/query/q/final");
}

#[test]
fn drained_results_are_bounded_by_count_bytes_and_ttl() {
    let later = Instant::now() + Duration::from_secs(60);
    let mut d = Drained::default();
    for i in 0..MAX_DRAINED {
        assert!(d.insert(format!("q{i}"), later, stored(1, 1)).is_empty());
    }
    assert_eq!(d.insert("over".into(), later, stored(1, 1)), ["q0"]);
    assert!(!d.contains("q0") && d.contains("q1") && d.contains("over"));
    // Taken entries leave stale order slots that never evict a live entry by mistake.
    assert!(d.remove("q1").is_some());
    assert!(d.insert("next".into(), later, stored(1, 1)).is_empty());
    assert!(d.contains("q2"));

    let mut d = Drained::default();
    d.insert("a".into(), later, stored(1, MAX_DRAINED_BYTES / 2));
    d.insert("b".into(), later, stored(1, MAX_DRAINED_BYTES / 2));
    assert_eq!(d.insert("c".into(), later, stored(1, 1)), ["a"]);
    assert_eq!(d.bytes, MAX_DRAINED_BYTES / 2 + 1);

    let mut d = Drained::default();
    let soon = Instant::now() + Duration::from_secs(5);
    assert!(d.insert("old".into(), soon, stored(1, 5)).is_empty());
    assert!(d.prune(soon - Duration::from_secs(1)).is_empty());
    assert_eq!(d.prune(soon), ["old"]);
    assert_eq!(d.bytes, 0);
}
