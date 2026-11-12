//! JSON-lines experiment driver for a live Databend capture process.
//! Stdin/out are local test orchestration, NOT an authenticated application protocol.
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use clap::Parser;
use dataflow_state::{DurabilityMode, PersistenceParameters};
use readyset_client::recipe::ChangeList;
use readyset_client::{ReaderHandle, ReadySetHandle};
use readyset_data::{DfValue, Dialect};
use readyset_databend::cache_control::{self, CacheControl};
use readyset_databend::declaration::{Declaration, Template};
use readyset_databend::{Batch, Sink, SourceRow, delivery};
use readyset_server::{Builder, Handle};
use replication_offset::ReplicationOffset;
use replication_offset::databend::DatabendPosition;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use readyset_alloc as _;

const TABLE: &str = "m2_source";
const QUERY: &str = "m2_by_id";

#[derive(Parser)]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    generation: String,
    /// Optional explicit source relation/query declarations for the HTTP gateway.
    #[arg(long)]
    declarations: Option<PathBuf>,
    /// Validate declarations without starting ReadySet or touching the output directory.
    #[arg(long)]
    validate_only: bool,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Apply {
        batch: Batch,
    },
    Verify {
        label: String,
        rows: Vec<SourceRow>,
        missing: Vec<i64>,
    },
    Query {
        sql: String,
        database: String,
    },
    Barrier {
        rows: Vec<SourceRow>,
        missing: Vec<i64>,
    },
    CacheDdl {
        sql: String,
        database: String,
    },
    ResetSink,
    Checkpoint,
    Shutdown,
}

async fn emit(value: &Value) -> Result<()> {
    let mut output = tokio::io::stdout();
    output
        .write_all(serde_json::to_string(value)?.as_bytes())
        .await?;
    output.write_all(b"\n").await?;
    output.flush().await?;
    Ok(())
}

async fn wait_rows(reader: &mut ReaderHandle, count: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if reader.len().await? == count {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("reader row-count convergence timeout")?
}

async fn verify(
    reader: &mut ReaderHandle,
    label: String,
    rows: Vec<SourceRow>,
    missing: Vec<i64>,
) -> Result<Value> {
    ensure!(
        rows.len() + missing.len() <= 20000,
        "verification request too large"
    );
    let ids: HashSet<_> = rows.iter().map(|r| r.id).collect();
    ensure!(ids.len() == rows.len(), "expected source keys not unique");
    ensure!(
        missing.iter().all(|id| !ids.contains(id)),
        "conflicting expected/missing keys"
    );
    let started = Instant::now();
    // ReaderHandle::len counts key slots; deleted keys may retain empty slots.
    // Validate live contents instead of equating that size to the source row count.
    for (id, expected) in rows
        .iter()
        .map(|r| (r.id, vec![r.values()]))
        .chain(missing.iter().map(|&id| (id, vec![])))
    {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let actual = reader
                    .lookup(&[id.into()], Dialect::DEFAULT_MYSQL)
                    .await?
                    .into_vec();
                if actual == expected {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .with_context(|| format!("query result mismatch for {label}, key {id}"))??;
    }
    let keys = reader.keys().await?;
    let mut extra_keys_checked = 0;
    for key in &keys {
        ensure!(key.len() == 1, "unexpected composite reader key");
        let id = i64::try_from(key[0].clone())?;
        if !ids.contains(&id) {
            let actual = reader.lookup(key, Dialect::DEFAULT_MYSQL).await?.into_vec();
            ensure!(
                actual.is_empty(),
                "unexpected extra live row at key {id}: {actual:?}"
            );
            extra_keys_checked += 1;
        }
    }
    Ok(
        json!({"ok":true,"label":label,"rows_verified":rows.len(),"missing_verified":missing.len(),
              "live_rows_verified":rows.len(),"reader_key_slots":reader.len().await?,
              "extra_keys_checked":extra_keys_checked,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0}),
    )
}

fn full_projection(template: &Template) -> bool {
    template.projection == ["id", "payload", "score", "note"]
}

async fn create_reader(handle: &mut Handle, template: &Template) -> Result<ReaderHandle> {
    // ReadySet removes ALL aliases of an identical query on DROP. Reuse the internal
    // full-row reader directly, rather than adding a droppable alias to the barrier query.
    let name = if full_projection(template) {
        QUERY
    } else {
        handle
            .extend_recipe(ChangeList::from_strings(
                vec![format!(
                    "CREATE CACHE {} FROM {}",
                    template.name, template.local_sql
                )],
                Dialect::DEFAULT_MYSQL,
            )?)
            .await?;
        template.name.as_str()
    };
    let reader = handle
        .view(name)
        .await?
        .into_reader_handle()
        .context("single declared reader required")?;
    ensure!(
        handle
            .materialization_info()
            .await?
            .iter()
            .all(|m| !m.partial),
        "partial query state"
    );
    Ok(reader)
}

async fn fence_template(
    template: &Template,
    reader: &mut ReaderHandle,
    rows: &[SourceRow],
    missing: &[i64],
) -> Result<()> {
    for (id, expected) in rows
        .iter()
        .map(|row| {
            let values = row.values();
            let projected = template
                .projection
                .iter()
                .map(|c| {
                    values[match c.as_str() {
                        "id" => 0,
                        "payload" => 1,
                        "score" => 2,
                        "note" => 3,
                        _ => unreachable!(),
                    }]
                    .clone()
                })
                .collect::<Vec<_>>();
            (row.id, vec![projected])
        })
        .chain(missing.iter().map(|&id| (id, vec![])))
    {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if reader
                    .lookup(&[id.into()], Dialect::DEFAULT_MYSQL)
                    .await?
                    .into_vec()
                    == expected
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("declared reader barrier timeout")??;
    }
    Ok(())
}

fn cache_listing(templates: &[Template]) -> Value {
    json!({"ok":true,"schema":[{"name":"name","type":"String"},
        {"name":"query","type":"String"},{"name":"state","type":"String"}],
        "data":templates.iter().map(|t| vec![t.name.clone(),t.source_sql.clone(),"ready".into()]).collect::<Vec<_>>(),
        "caches":templates.iter().map(|t| t.name.clone()).collect::<Vec<_>>()})
}

async fn experiment(args: &Args, handle: &mut Handle) -> Result<Value> {
    let declaration: Option<Declaration> = args
        .declarations
        .as_ref()
        .map(|p| -> Result<_> { Ok(serde_json::from_reader(File::open(p)?)?) })
        .transpose()?;
    let mut templates = declaration
        .as_ref()
        .map(|d| d.templates(TABLE))
        .transpose()?
        .unwrap_or_default();
    handle.backend_ready().await;
    handle.extend_recipe(ChangeList::from_strings(vec![format!(
        "CREATE TABLE {TABLE} (id BIGINT NOT NULL, payload TEXT NOT NULL, score BIGINT NOT NULL, note TEXT, PRIMARY KEY(id))"
    )], Dialect::DEFAULT_MYSQL)?).await?;
    let rows: Vec<SourceRow> = BufReader::new(File::open(&args.input)?)
        .lines()
        .map(|line| Ok(serde_json::from_str(&line?)?))
        .collect::<Result<_>>()?;
    ensure!(
        !rows.is_empty() && rows.len() <= 10000,
        "M2 initial row count must be 1..10000"
    );
    ensure!(
        rows.iter().map(|r| r.id).collect::<HashSet<_>>().len() == rows.len(),
        "duplicate initial key"
    );
    let mut table = handle.table(TABLE).await?;
    for chunk in rows.chunks(1000) {
        table
            .insert_many(chunk.iter().map(SourceRow::values))
            .await?;
    }
    table
        .set_replication_offset(ReplicationOffset::Databend(DatabendPosition::new(
            &args.generation,
            0,
        )?))
        .await?;
    let mut sink = Sink::new(args.generation.clone(), TABLE.into())?;
    sink.wait_persisted(handle, 0).await?;
    handle.extend_recipe(ChangeList::from_strings(vec![format!(
        "CREATE CACHE {QUERY} FROM SELECT id, payload, score, note FROM {TABLE} WHERE id = ?"
    )], Dialect::DEFAULT_MYSQL)?).await?;
    let mut reader = handle
        .view(QUERY)
        .await?
        .into_reader_handle()
        .context("single reader required")?;
    wait_rows(&mut reader, rows.len()).await?;
    ensure!(
        handle
            .materialization_info()
            .await?
            .iter()
            .all(|m| !m.partial),
        "partial query state"
    );
    let mut declared_readers = Vec::new();
    for template in &templates {
        let mut r = create_reader(handle, template).await?;
        fence_template(template, &mut r, &rows, &[]).await?;
        declared_readers.push(r);
    }
    let mut barrier_rows = rows;
    let mut barrier_missing = Vec::new();
    let mut barrier_checkpoint = 0;
    emit(&json!({"ok":true,"ready":true,"rows":barrier_rows.len(),"checkpoint":0,"generation":args.generation})).await?;
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut results = Vec::new();
    loop {
        let line = tokio::time::timeout(
            Duration::from_secs(if declaration.is_some() { 86400 } else { 120 }),
            lines.next_line(),
        )
        .await
        .context("timed out waiting for M2 command")??
        .context("stdin closed without shutdown")?;
        ensure!(
            line.len() <= 8 * 1024 * 1024,
            "command exceeds experiment limit"
        );
        let command: Command = serde_json::from_str(&line)?;
        let response = match command {
            Command::Apply { batch } => {
                let current = sink.checkpoint(handle).await?;
                // Only preflight failures are recoverable in this test protocol. An uncertain
                // Table RPC/persistence error exits the runner; it is never blindly retried.
                match delivery(&current, &batch) {
                    Err(error) => {
                        json!({"ok":false,"rejected":true,"error":format!("{error:#}"),"checkpoint":current.sequence()})
                    }
                    Ok(_) => sink.apply(handle, &mut table, &batch).await?,
                }
            }
            Command::Verify {
                label,
                rows,
                missing,
            } => verify(&mut reader, label, rows, missing).await?,
            Command::Query { sql, database } => {
                let found = declaration.as_ref().and_then(|d| {
                    templates
                        .iter()
                        .enumerate()
                        .find_map(|(i, t)| t.match_key(&sql, &database, d).map(|key| (i, key)))
                });
                if let Some((i, key)) = found {
                    let t = &templates[i];
                    let actual = declared_readers[i]
                        .lookup(&[key.into()], Dialect::DEFAULT_MYSQL)
                        .await?
                        .into_vec();
                    let data = actual
                        .iter()
                        .map(|row| {
                            row.iter()
                                .zip(&t.projection)
                                .map(|(v, c)| -> Result<Value> {
                                    Ok(match v {
                                        DfValue::None => Value::Null,
                                        _ if c == "id" || c == "score" => {
                                            Value::String(i64::try_from(v.clone())?.to_string())
                                        }
                                        _ => Value::String(
                                            v.as_str().context("non-text projected value")?.into(),
                                        ),
                                    })
                                })
                                .collect::<Result<Vec<_>>>()
                        })
                        .collect::<Result<Vec<_>>>()?;
                    json!({"ok":true,"matched":true,"template":t.name,"schema":t.schema(),"data":data})
                } else {
                    json!({"ok":true,"matched":false})
                }
            }
            Command::Barrier { rows, missing } => {
                let result = verify(
                    &mut reader,
                    "replication barrier".into(),
                    rows.clone(),
                    missing.clone(),
                )
                .await?;
                // Keep a fenced oracle for runtime cache construction. Never rescan the source.
                for (t, r) in templates.iter().zip(&mut declared_readers) {
                    fence_template(t, r, &rows, &missing).await?;
                }
                barrier_rows = rows;
                barrier_missing = missing;
                barrier_checkpoint = sink.checkpoint(handle).await?.sequence();
                result
            }
            Command::CacheDdl { sql, database } => {
                let planned = declaration
                    .as_ref()
                    .context("source binding required")
                    .and_then(|d| cache_control::plan(&sql, &database, d, &templates));
                match planned {
                    Err(error) => json!({"ok":false,"rejected":true,"error":format!("{error:#}")}),
                    Ok(control) => {
                        // After this point, an uncertain migration/RPC failure is fatal.
                        // Do not publish the new route or retry a mutation with unknown outcome.
                        ensure!(
                            sink.checkpoint(handle).await?.sequence() == barrier_checkpoint,
                            "cache DDL requires a completed replication barrier"
                        );
                        match control {
                            CacheControl::Create(template) => {
                                sink.wait_persisted(handle, barrier_checkpoint).await?;
                                let mut r = create_reader(handle, &template).await?;
                                fence_template(&template, &mut r, &barrier_rows, &barrier_missing)
                                    .await?;
                                templates.push(*template);
                                declared_readers.push(r);
                            }
                            CacheControl::Drop(name) => {
                                let i = templates.iter().position(|t| t.name == name).unwrap();
                                if !full_projection(&templates[i]) {
                                    ensure!(
                                        handle.remove_query(&name.into()).await? == 1,
                                        "cache missing in controller"
                                    );
                                }
                                templates.remove(i);
                                declared_readers.remove(i);
                                // A drop must not invalidate any surviving query or the shared base.
                                verify(
                                    &mut reader,
                                    "post-drop barrier".into(),
                                    barrier_rows.clone(),
                                    barrier_missing.clone(),
                                )
                                .await?;
                                for (t, r) in templates.iter().zip(&mut declared_readers) {
                                    fence_template(t, r, &barrier_rows, &barrier_missing).await?;
                                }
                            }
                            CacheControl::Show => {}
                        }
                        cache_listing(&templates)
                    }
                }
            }
            Command::ResetSink => {
                sink = Sink::new(args.generation.clone(), TABLE.into())?;
                // Recreate controller client and table mutator too. Durable engine metadata,
                // not any counter in the old sink/client, determines the next sequence.
                let mut raw = ReadySetHandle::make_raw(
                    handle.get_address().clone(),
                    Some(Duration::from_secs(30)),
                    None,
                );
                let current = sink.checkpoint(&mut raw).await?;
                table = raw.table(TABLE).await?;
                json!({"ok":true,"reset_sink":true,"checkpoint":current.sequence()})
            }
            Command::Checkpoint => {
                let current = sink.checkpoint(handle).await?;
                json!({"ok":true,"checkpoint":current.sequence()})
            }
            Command::Shutdown => {
                emit(&json!({"ok":true,"shutdown":true})).await?;
                break;
            }
        };
        emit(&response).await?;
        if results.len() < 1000 {
            results.push(response);
        }
    }
    Ok(
        json!({"passed":true,"generation":args.generation,"commands":results,
              "checkpoint":sink.checkpoint(handle).await?.sequence(),
              "materializations":handle.materialization_info().await?,
              "persistent_base":true,"single_serial_consumer":true,
              "limitations":["No process/node crash test", "No multi-writer sink", "No SQL wire frontend", "No outbox cleanup"]}),
    )
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    if args.validate_only {
        let path = args
            .declarations
            .as_ref()
            .context("--declarations required for validation")?;
        let declaration: Declaration = serde_json::from_reader(File::open(path)?)?;
        declaration.templates(TABLE)?;
        return Ok(());
    }
    ensure!(!args.output.exists(), "output directory must not exist");
    DatabendPosition::new(&args.generation, 0)?;
    fs::create_dir_all(&args.output)?;
    let mut builder = Builder::default();
    builder.set_external_addr("127.0.0.1:0".parse()?);
    builder.set_dialect(readyset_sql::Dialect::MySQL);
    builder.disable_partial();
    // No upstream URL; do not use replication_enabled=false (shallow-only deep-state wipe).
    builder.set_persistence(PersistenceParameters {
        mode: DurabilityMode::Permanent,
        db_filename_prefix: "m2".into(),
        storage_dir: Some(args.output.join("storage")),
        // Use the tracked background WAL-sync path. The synchronous (0) path currently
        // leaves the durability watermark at Unflushed after offset-bearing writes.
        wal_flush_interval_seconds: 1,
        ..Default::default()
    });
    let (mut handle, shutdown) = builder.start_local().await?;
    let outcome = tokio::time::timeout(
        Duration::from_secs(if args.declarations.is_some() {
            86400
        } else {
            600
        }),
        experiment(&args, &mut handle),
    )
    .await
    .context("M2 experiment timeout")
    .and_then(|r| r);
    let report = match &outcome {
        Ok(r) => r.clone(),
        Err(error) => json!({"passed":false,"error":format!("{error:#}")}),
    };
    fs::write(
        args.output.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    drop(handle);
    shutdown.shutdown_timeout(Duration::from_secs(30)).await;
    outcome?;
    Ok(())
}
