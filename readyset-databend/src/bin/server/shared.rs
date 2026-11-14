//! Shared native runtime orchestration. HTTP auth, session checks, result lifecycle and
//! proxying remain in the existing front end; only source ownership/state is per table.
use super::*;
use readyset_databend::replicator::parse_typed_changes;
use readyset_databend::restart_marker::Decision;
use readyset_databend::shared_engine::SharedEngine;

/// An empty consuming read is normally rolled back (no Stream or marker write). Commit one
/// at least this often so a quiet table's Stream offset keeps up with source snapshots.
const EMPTY_COMMIT_INTERVAL: Duration = Duration::from_secs(60);

pub(super) async fn run_shared(s: Settings) -> Result<()> {
    fs::create_dir_all(&s.run_dir)?;
    let journal = Journal::open(&s.run_dir.join("source_queries.jsonl"))?;
    let http = http_client()?;
    let auth = basic_auth(&s.user, &s.password);
    let generation = format!("rs_{}_{}", s.instance_id, new_id(12));
    let mut tables = std::collections::BTreeMap::new();
    for (db, table) in &s.tables {
        let names = Names::new(db, table, &s.instance_id, &generation);
        tables.insert(
            names.source.clone(),
            Arc::new(TableState {
                syncing: std::sync::atomic::AtomicBool::new(false),
                names,
                state: StdMutex::new(RouteState::default()),
            }),
        );
    }
    // Exactly ONE native server. Its standalone authority and base RocksDB live in a stable
    // directory per instance, so a restart reopens rows, caches and restart markers. This
    // needs no Databend: tables attach and recover in the background below.
    let engine = SharedEngine::start(
        s.engine_dir.as_path(),
        &format!("rs_{}", s.instance_id),
        s.cache,
        tables.keys().cloned().collect(),
        vec![],
    )
    .await?;
    let names = tables
        .values()
        .next()
        .context("no configured source")?
        .names
        .clone();
    let (stop, _) = watch::channel(false);
    let routes_journal = if s.route_journal {
        Some(Journal::open(&s.run_dir.join("routes.jsonl"))?)
    } else {
        None
    };
    let app = Arc::new(App {
        s,
        auth,
        http,
        engine: CacheEngine::Shared(Arc::clone(&engine)),
        tables,
        names,
        default_role: StdMutex::new(None),
        state: StdMutex::new(RouteState::default()),
        results: StdMutex::new(HashMap::new()),
        passive: StdMutex::new(HashMap::new()),
        control: tokio::sync::Mutex::new(()),
        capturing: tokio::sync::Mutex::new(()),
        routes_journal,
        stop,
    });
    // HTTP starts immediately: until a table is ready its reads go to Databend, so a long
    // snapshot or rebuild never takes the whole service (or its liveness probe) down.
    let workers = app
        .tables
        .iter()
        .map(|(id, table)| {
            let capture = {
                let (app, names, journal) =
                    (Arc::clone(&app), table.names.clone(), Arc::clone(&journal));
                move || {
                    let client = |label: &str| {
                        DatabendClient::new(
                            app.http.clone(),
                            &app.s.endpoint,
                            &app.auth,
                            label,
                            Some(Arc::clone(&journal)),
                        )
                    };
                    Replicator::new(names.clone(), client("bootstrap"), client("capture"), false)
                }
            };
            tokio::spawn(table_worker(
                Arc::clone(&app),
                Arc::clone(&engine),
                id.clone(),
                capture,
            ))
        })
        .collect();
    app.write_status("status.json");
    serve(app, workers).await
}

/// Longest wait between recovery attempts of one failed table.
const RETRY_MAX: Duration = Duration::from_secs(300);
/// A table that stayed healthy this long starts its next failure at the initial backoff.
const HEALTHY_RESET: Duration = Duration::from_secs(300);

/// One table's whole life: recover (resume / create / rebuild only this table), sync until
/// a failure, then back off and recover again. Any error is confined to this table: its
/// reads go to Databend, other tables keep syncing and serving. A fresh capture session is
/// used for every attempt, so a failed or uncertain Databend transaction is never reused.
async fn table_worker(
    app: Arc<App>,
    engine: Arc<SharedEngine>,
    id: String,
    new_capture: impl Fn() -> Replicator,
) {
    let mut stop = app.stop.subscribe();
    let mut backoff = app.s.retry_backoff;
    let mut attempt = 0u64;
    loop {
        if *stop.borrow() {
            return;
        }
        attempt += 1;
        let mut capture = new_capture();
        let healthy_since = match recover(&app, &engine, &id, &mut capture, attempt).await {
            Ok(()) => Some(Instant::now()),
            Err(e) => {
                app.fail_source(&id, format!("recovery failed (attempt {attempt}): {e:#}"));
                None
            }
        };
        if healthy_since.is_some() {
            loop {
                tokio::select! { _=tokio::time::sleep(app.s.interval)=>{}, _=stop.changed()=>{} }
                if *stop.borrow() {
                    return;
                }
                // An external fail-closed (fault injection, engine) also ends this run.
                let failed = app.tables[&id].lock().failed.clone();
                let result = match failed {
                    Some(reason) => Err(anyhow::anyhow!("{reason}")),
                    None => sync_source(&app, &engine, &id, &mut capture).await,
                };
                if let Err(e) = result {
                    app.fail_source(&id, format!("sync failure: {e:#}"));
                    break;
                }
            }
        }
        app.write_status("status.json");
        if healthy_since.is_some_and(|t| t.elapsed() >= HEALTHY_RESET) {
            backoff = app.s.retry_backoff;
        }
        tokio::select! { _=tokio::time::sleep(backoff)=>{}, _=stop.changed()=>{} }
        backoff = (backoff * 2).min(RETRY_MAX);
    }
}

/// Bring one table to a consistent, synced state from whatever it was: discover its schema
/// (and Stream), (re)attach it to the native runtime, then resume or reload it according
/// to its restart marker, and run the first round. The table stays failed (reads upstream)
/// until the first round succeeds.
async fn recover(
    app: &App,
    engine: &Arc<SharedEngine>,
    id: &str,
    capture: &mut Replicator,
    attempt: u64,
) -> Result<()> {
    let started = Instant::now();
    let schema = capture.prepare_schema().await?;
    if let Some(role) = &capture.default_role {
        app.default_role
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert_with(|| role.clone());
    }
    engine.attach(schema).await?;
    let (stream, version) = capture.stream_version().await?;
    let (rows, recovery) = match engine.decide(id, stream, version)? {
        Decision::Resume(version) => {
            engine.resume(id, stream, version).await?;
            (capture.resume().await?, "resumed".to_string())
        }
        decision => {
            let reason = match decision {
                Decision::Rebuild(reason) => format!("rebuilt: {reason}"),
                _ => "created".into(),
            };
            let rebuild = matches!(decision, Decision::Rebuild(_));
            // The local table is rebuilt from scratch; only after a schema change must the
            // Stream first move past the DDL so the snapshot can read the new columns.
            // Crash after this and before the load: the old digest still forces a rebuild.
            let stream = if decision == Decision::Rebuild("source schema changed") {
                capture.fast_forward().await?;
                capture.stream_version().await?.0
            } else {
                stream
            };
            match engine.snapshot(id, capture, rebuild, stream).await {
                Ok(rows) => (rows, reason),
                Err(e) => return Err(capture.abort_capture(e).await),
            }
        }
    };
    {
        let mut state = app.tables[id].lock();
        let previous = state.failed.take();
        state.rows = rows;
        state.snapshot = json!({"rows":rows,"recovery":recovery,"attempt":attempt,
            "previous_failure":previous,"total_secs":started.elapsed().as_secs_f64()});
    }
    if let Err(e) = sync_source(app, engine, id, capture).await {
        return Err(e.context("first round after recovery"));
    }
    app.write_status("status.json");
    Ok(())
}

struct Polling<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

async fn sync_source(
    app: &App,
    engine: &Arc<SharedEngine>,
    id: &str,
    capture: &mut Replicator,
) -> Result<()> {
    let polling = &app.tables[id].syncing;
    polling.store(true, std::sync::atomic::Ordering::Release);
    let _polling = Polling(polling);
    // The first round consumes the source snapshot pinned at bootstrap start, not at
    // this function's entry. A subsequent fresh transaction catches up later writes.
    let started = capture.bootstrap_started().unwrap_or_else(Instant::now);
    let (epoch, clean) = {
        let state = app.lock();
        (
            state.forward_epoch,
            state.active_forwards == 0 && state.inflight.is_empty() && !state.uncertain,
        )
    };
    let result = source_round(engine, id, capture).await;
    let summary = match result {
        Ok(value) => value,
        Err(e) => return Err(capture.abort_capture(e).await),
    };
    {
        let mut global = app.lock();
        let mut state = app.tables[id].lock();
        state.sequence = capture.sequence;
        state.rows = capture.rows;
        state.last_sync = Some(started);
        state.counters.sync_batches += 1;
        global.counters.sync_batches += 1;
        if summary["changes"].as_u64().unwrap_or(0) > 0 {
            state.last_batch = summary;
        }
        state.forward_epoch = epoch;
        state.ready = state.failed.is_none()
            && clean
            && epoch == global.forward_epoch
            && global.active_forwards == 0
            && global.inflight.is_empty()
            && !global.uncertain
            && started.elapsed() <= app.s.max_staleness;
    }
    app.write_status("status.json");
    Ok(())
}

async fn source_round(
    engine: &Arc<SharedEngine>,
    id: &str,
    capture: &mut Replicator,
) -> Result<Value> {
    let started = Instant::now();
    let schema = capture.schema.clone().context("source schema missing")?;
    let sequence = capture.sequence + 1;
    let mut pages = capture.begin_consume().await?;
    let read = async {
        let mut page = pages.next().await?;
        let mut writer = engine.begin_batch(id, sequence).await?;
        let mut changes = 0usize;
        while let Some(rows) = page {
            changes += rows.len();
            writer.push(parse_typed_changes(&schema, &rows)?).await?;
            page = pages.next().await?;
        }
        Ok::<_, anyhow::Error>((writer, changes))
    }
    .await;
    let (writer, changes) = match read {
        Ok(r) => r,
        Err(e) => return Err(pages.abort(e).await),
    };
    // Complete consuming read required, before native checkpoint/barrier and COMMIT.
    pages.close().await?;
    capture.check_schema().await?;
    let (inserts, deletes, cancelled) = (writer.inserts, writer.deletes, writer.cancelled);
    let bootstrap = capture.bootstrap_started().is_some();
    let mut writer = writer;
    let commit = changes > 0 || (!bootstrap && capture.since_commit() >= EMPTY_COMMIT_INTERVAL);
    if commit {
        // Before COMMIT the restart marker must say Applying: if the process dies after the
        // Stream moved, the next start can tell the batch was complete.
        writer.mark_applying().await?;
    }
    let finish = writer.finish().await?;
    if commit {
        capture.commit_consumed(inserts, deletes).await?;
    } else {
        capture.rollback_empty().await?;
    }
    if commit || bootstrap {
        // Read-only and safe to retry; the Stream only moves by this instance's COMMITs.
        let mut attempt = 0;
        let (stream, version) = loop {
            match capture.stream_version().await {
                Ok(v) => break v,
                Err(e) if attempt >= 2 => return Err(e.context("stream version after commit")),
                Err(_) => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                }
            }
        };
        engine.committed(id, stream, version).await?;
    }
    capture.check_schema().await?;
    Ok(
        json!({"sequence":sequence,"changes":changes,"inserts":inserts,"deletes":deletes,"cancelled":cancelled,
        "committed":commit,"finish":finish,"total_ms":started.elapsed().as_secs_f64()*1000.0}),
    )
}
