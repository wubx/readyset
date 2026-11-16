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
