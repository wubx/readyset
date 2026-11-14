//! M2 stdin/stdout experiment runner around the shared in-process engine.
//! Protocol and JSON responses are unchanged; the ReadySet logic lives in `engine.rs`.
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use clap::Parser;
use readyset_databend::declaration::Declaration;
use readyset_databend::engine::{Engine, EngineConfig, ReadOutcome};
use readyset_databend::native_query::LOCAL_TABLE;
use readyset_databend::{Batch, SourceRow};
use replication_offset::databend::DatabendPosition;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use readyset_alloc as _;

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

async fn experiment(args: &Args, engine: &Engine, interactive: bool) -> Result<Value> {
    emit(&json!({"ok":true,"ready":true,"checkpoint":0,"generation":args.generation})).await?;
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut results = Vec::new();
    loop {
        let line = tokio::time::timeout(
            Duration::from_secs(if interactive { 86400 } else { 120 }),
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
            Command::Apply { batch } => engine.apply(&batch).await?,
            Command::Verify {
                label,
                rows,
                missing,
            } => engine.verify(label, rows, missing).await?,
            Command::Query { sql, database } => match engine.query(&sql, &database).await? {
                ReadOutcome::Matched {
                    template,
                    schema,
                    data,
                } => {
                    json!({"ok":true,"matched":true,"template":template,"schema":schema,"data":data})
                }
                ReadOutcome::LimitMiss => {
                    json!({"ok":true,"matched":false,"reason":"local result limit"})
                }
                ReadOutcome::NotMatched => json!({"ok":true,"matched":false}),
            },
            Command::Barrier { rows, missing } => engine.barrier(rows, missing).await?,
            Command::CacheDdl { sql, database } => engine.cache_ddl(&sql, &database).await?,
            Command::ResetSink => engine.reset_sink().await?,
            Command::Checkpoint => json!({"ok":true,"checkpoint":engine.checkpoint().await?}),
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
              "checkpoint":engine.checkpoint().await?,
              "materializations":engine.materializations().await?,
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
    let declaration: Option<Declaration> = args
        .declarations
        .as_ref()
        .map(|p| -> Result<_> { Ok(serde_json::from_reader(File::open(p)?)?) })
        .transpose()?;
    if args.validate_only {
        declaration
            .as_ref()
            .context("--declarations required for validation")?
            .templates(LOCAL_TABLE)?;
        return Ok(());
    }
    ensure!(!args.output.exists(), "output directory must not exist");
    DatabendPosition::new(&args.generation, 0)?;
    fs::create_dir_all(&args.output)?;
    let rows: Vec<SourceRow> = BufReader::new(File::open(&args.input)?)
        .lines()
        .map(|line| Ok(serde_json::from_str(&line?)?))
        .collect::<Result<_>>()?;
    ensure!(
        rows.iter().map(|r| r.id).collect::<HashSet<_>>().len() == rows.len(),
        "duplicate initial key"
    );
    let interactive = declaration.is_some();
    let started = Engine::start(
        EngineConfig {
            generation: args.generation.clone(),
            storage_dir: args.output.join("storage"),
            declaration,
            // databend-m2 drives the POC content oracle through its stdin protocol.
            oracle: true,
            cache: Default::default(),
        },
        rows,
    )
    .await;
    let outcome = match &started {
        Ok(engine) => tokio::time::timeout(
            Duration::from_secs(if interactive { 86400 } else { 600 }),
            experiment(&args, engine, interactive),
        )
        .await
        .context("M2 experiment timeout")
        .and_then(|r| r),
        Err(error) => Err(anyhow::anyhow!("{error:#}")),
    };
    let report = match &outcome {
        Ok(r) => r.clone(),
        Err(error) => json!({"passed":false,"error":format!("{error:#}")}),
    };
    fs::write(
        args.output.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    if let Ok(engine) = &started {
        engine.shutdown().await;
    }
    outcome?;
    Ok(())
}
