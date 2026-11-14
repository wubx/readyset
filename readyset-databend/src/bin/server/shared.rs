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
    let client = |label: &str| {
        DatabendClient::new(
            http.clone(),
            &s.endpoint,
            &auth,
            label,
            Some(Arc::clone(&journal)),
        )
    };
    let mut tables = std::collections::BTreeMap::new();
    let mut captures = Vec::new();
    let mut schemas = Vec::new();
    let mut default_role = None;
    for (db, table) in &s.tables {
        let names = Names::new(db, table, &s.instance_id, &generation);
        let id = names.source.clone();
        let mut capture =
            Replicator::new(names.clone(), client("bootstrap"), client("capture"), false);
        let prepared = capture.prepare_schema().await;
        if default_role.is_none() {
            default_role = capture.default_role.clone();
        }
        let mut state = RouteState::default();
        match prepared {
            Ok(schema) => {
                schemas.push(schema);
                captures.push((id.clone(), capture));
            }
            Err(e) => {
                state.failed = Some(format!("source preparation failed: {e:#}"));
            }
        }
        tables.insert(
            id,
            Arc::new(TableState {
                syncing: std::sync::atomic::AtomicBool::new(false),
                names,
                state: StdMutex::new(state),
            }),
        );
    }
    // Exactly ONE native server. Its standalone authority and base RocksDB live in a stable
    // directory per instance, so a restart reopens rows, caches and restart markers.
    let engine = SharedEngine::start(
        s.engine_dir.as_path(),
        &format!("rs_{}", s.instance_id),
        s.cache,
        schemas,
    )
    .await?;
    for (id, capture) in &mut captures {
        let started = Instant::now();
        let decision = match capture.stream_version().await {
            Ok(version) => engine.decide(id, version),
            Err(e) => Err(e),
        };
        let recovered = match decision {
            Ok(Decision::Resume(version)) => async {
                engine.resume(id, version).await?;
                capture.resume().await
            }
            .await
            .map(|rows| (rows, "resumed".to_string())),
            Ok(decision) => {
                let reason = match decision {
                    Decision::Rebuild(reason) => format!("rebuilt: {reason}"),
                    _ => "created".into(),
                };
                let rebuild = matches!(decision, Decision::Rebuild(_));
                match engine.snapshot(id, capture, rebuild).await {
                    Ok(rows) => Ok((rows, reason)),
                    Err(e) => Err(capture.abort_capture(e).await),
                }
            }
            Err(e) => Err(e),
        };
        match recovered {
            Ok((rows, recovery)) => {
                let mut state = tables[id].lock();
                state.rows = rows;
                state.snapshot = json!({"rows":rows,"recovery":recovery,"total_secs":started.elapsed().as_secs_f64()});
            }
            Err(e) => {
                tables[id].lock().failed = Some(format!("startup recovery failed: {e:#}"));
                engine.fail_source(id);
            }
        }
    }
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
        default_role,
        state: StdMutex::new(RouteState::default()),
        results: StdMutex::new(HashMap::new()),
        passive: StdMutex::new(HashMap::new()),
        control: tokio::sync::Mutex::new(()),
        capturing: tokio::sync::Mutex::new(()),
        routes_journal,
        stop,
    });
    let mut workers = Vec::new();
    for (id, mut capture) in captures {
        if app.tables[&id].lock().failed.is_some() {
            continue;
        }
        if let Err(e) = sync_source(&app, &engine, &id, &mut capture).await {
            app.fail_source(&id, format!("initial capture failed: {e:#}"));
            continue;
        }
        let app = Arc::clone(&app);
        let engine = Arc::clone(&engine);
        let mut stop = app.stop.subscribe();
        workers.push(tokio::spawn(async move {
            loop {
                tokio::select! { _=tokio::time::sleep(app.s.interval)=>{}, _=stop.changed()=>{} }
                if *stop.borrow() || app.tables[&id].lock().failed.is_some() {
                    break;
                }
                if let Err(e) = sync_source(&app, &engine, &id, &mut capture).await {
                    app.fail_source(&id, format!("sync failure: {e:#}"));
                    app.write_status("status.json");
                    break;
                }
            }
        }));
    }
    app.write_status("status.json");
    serve(app, workers).await
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
        let version = loop {
            match capture.stream_version().await {
                Ok(v) => break v,
                Err(e) if attempt >= 2 => return Err(e.context("stream version after commit")),
                Err(_) => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                }
            }
        };
        engine.committed(id, version).await?;
    }
    capture.check_schema().await?;
    Ok(
        json!({"sequence":sequence,"changes":changes,"inserts":inserts,"deletes":deletes,"cancelled":cancelled,
        "committed":commit,"finish":finish,"total_ms":started.elapsed().as_secs_f64()*1000.0}),
    )
}
