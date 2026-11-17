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
    st.inflight.insert("q".into(), now);
    assert_eq!(st.unavailable(now, max), Some("upstream_inflight"));
    st.mark_unresolved("u1");
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

#[test]
fn write_fence_is_per_session_and_needs_a_round_started_after_the_write() {
    let mut f = WriteFence::default();
    let round_before = f.snapshot();
    let e = f.begin(Some("s1"));
    assert!(f.pending(Some("s1")));
    assert!(!f.pending(Some("s2")), "other sessions keep local reads");
    assert!(
        f.pending(None),
        "a request without a session cannot prove it is not s1"
    );
    // A round that started before or during the write does not cover it.
    let round_during = f.snapshot();
    f.end(e);
    f.synced(round_before);
    assert!(f.pending(Some("s1")));
    f.synced(round_during);
    assert!(f.pending(Some("s1")));
    // A round that started after the write completed does.
    let round_after = f.snapshot();
    f.synced(round_after);
    assert!(!f.pending(Some("s1")) && !f.pending(None));
    assert_eq!(f.waiting(), 0);
}

#[test]
fn write_fence_covers_completed_writes_below_the_oldest_in_flight() {
    let mut f = WriteFence::default();
    let a = f.begin(Some("a"));
    let b = f.begin(Some("b"));
    let c = f.begin(Some("c"));
    f.end(a);
    f.end(c);
    // b still in flight: a round now covers a, not b, and not c (above b).
    f.synced(f.snapshot());
    assert!(!f.pending(Some("a")));
    assert!(f.pending(Some("b")) && f.pending(Some("c")));
    f.end(b);
    f.synced(f.snapshot());
    assert!(!f.pending(Some("b")) && !f.pending(Some("c")));
}

#[test]
fn write_fence_without_session_or_over_capacity_fences_everyone() {
    let mut f = WriteFence::default();
    let e = f.begin(None);
    assert!(f.pending(Some("anyone")));
    f.end(e);
    f.synced(f.snapshot());
    assert!(!f.pending(Some("anyone")));

    let mut f = WriteFence::default();
    for i in 0..MAX_SESSION_FENCES {
        let e = f.begin(Some(&format!("s{i}")));
        f.end(e);
    }
    let e = f.begin(Some("one-too-many"));
    assert!(f.pending(Some("unrelated")));
    f.end(e);
    f.synced(f.snapshot());
    assert!(!f.pending(Some("unrelated")) && f.by_session.is_empty());
}

#[test]
fn cookie_session_reads_only_a_well_formed_session_id() {
    let headers = |v: &'static str| {
        let mut h = HeaderMap::new();
        h.insert(header::COOKIE, HeaderValue::from_static(v));
        h
    };
    assert_eq!(
        cookie_session(&headers(
            "cookie_enabled=true; session_id=01a1e704-dc6f; last_refresh_time=1"
        )),
        Some("01a1e704-dc6f".into())
    );
    assert_eq!(cookie_session(&headers("cookie_enabled=true")), None);
    assert_eq!(cookie_session(&headers("session_id=a b")), None);
    assert_eq!(cookie_session(&headers("session_id=a; session_id=b")), None);
    assert_eq!(cookie_session(&HeaderMap::new()), None);
}

#[test]
fn unresolved_outcomes_are_tracked_by_id_and_bounded() {
    let mut st = RouteState::default();
    assert!(!st.uncertain());
    st.mark_unresolved("rs_fwd_1");
    st.mark_unresolved("rs_fwd_1");
    assert!(st.uncertain() && st.unresolved.len() == 1 && st.counters.unresolved == 2);
    st.unresolved.remove("rs_fwd_1");
    assert!(!st.uncertain());
    // An ID that cannot be quoted into the resolver's SQL is never resolved.
    st.mark_unresolved("x'; DROP TABLE t; --");
    assert!(st.uncertain() && st.unresolved.is_empty() && st.unresolved_overflow);
    let mut st = RouteState::default();
    for i in 0..MAX_INFLIGHT {
        st.mark_unresolved(&format!("q{i}"));
    }
    assert!(!st.unresolved_overflow);
    st.mark_unresolved("one-more");
    assert!(st.unresolved_overflow);
}

#[test]
fn only_a_failed_start_without_a_query_is_rejected_before_start() {
    // Databend's reply to `{"sql":"SELECT 1","session":{"role":"no_such_role"}}`.
    let rejected = json!({"id": "", "session_id": null, "node_id": "", "state": "Failed",
        "error": {"code": 2206, "message": "Invalid role no_such_role"}});
    assert!(rejected_before_start(&rejected));
    let ran = json!({"id": "q1", "state": "Failed", "error": {"code": 1065, "message": "x"}});
    assert!(!rejected_before_start(&ran));
    assert!(!rejected_before_start(
        &json!({"id": "", "state": "Running", "error": null})
    ));
    assert!(!rejected_before_start(
        &json!({"id": "", "state": "Failed", "error": null})
    ));
    assert!(!rejected_before_start(&json!({})));
}

#[test]
fn query_ids_are_restricted_to_quotable_characters() {
    assert!(valid_query_id("d38e17ccb25e41dcacbff9e39d023a2d"));
    assert!(valid_query_id("rs_fwd_0123-abc"));
    assert!(!valid_query_id(""));
    assert!(!valid_query_id("a'b"));
    assert!(!valid_query_id(&"a".repeat(129)));
}
