//! Closed-loop point lookups over the Databend HTTP API.
//!
//! The same client drives Databend itself and readyset-databend (both speak `/v1/query`),
//! so the comparison includes HTTP, JSON, pagination and session handling on both sides.
//! Each worker sends one query, follows `next_uri` until the query is finished (as drivers
//! do), checks the row count, then sends the next. Keys come from a deterministic
//! splitmix64 sequence over `--key-space` (or the first `--hot` keys of it).
//!
//! Key kinds (must match the benchmark table's generator):
//! - `int`: the index itself;
//! - `ext`: `substr(sha2(to_string(i), 256), 1, 24)`;
//! - `mod`: `i % --modulus` (non-unique keys).
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use base64::Engine;
use clap::ValueEnum;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Kind {
    Int,
    Ext,
    Mod,
}

#[derive(clap::Args, Clone)]
pub struct Args {
    /// HTTP origin, e.g. http://127.0.0.1:8000
    #[arg(long)]
    endpoint: String,
    #[arg(long, default_value = "root")]
    user: String,
    /// SQL with `{k}` replaced by the key.
    #[arg(long)]
    template: String,
    #[arg(long, value_enum)]
    kind: Kind,
    #[arg(long)]
    key_space: u64,
    /// Draw keys only from the first N indexes (hot set).
    #[arg(long)]
    hot: Option<u64>,
    #[arg(long, default_value_t = 1_000_000)]
    modulus: u64,
    #[arg(long)]
    expect_rows: usize,
    #[arg(long)]
    concurrency: usize,
    #[arg(long, default_value_t = 30.0)]
    duration: f64,
    #[arg(long, default_value_t = 5.0)]
    warmup: f64,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Free-form label copied into the result line.
    #[arg(long, default_value = "")]
    label: String,
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn key(args: &Args, n: u64) -> String {
    let i = splitmix(n) % args.hot.unwrap_or(args.key_space).min(args.key_space);
    match args.kind {
        Kind::Int => i.to_string(),
        Kind::Ext => hex::encode(Sha256::digest(i.to_string().as_bytes()))[..24].to_owned(),
        Kind::Mod => (i % args.modulus).to_string(),
    }
}

#[derive(Default)]
struct Outcome {
    lat_us: Vec<u32>,
    errors: BTreeMap<String, u64>,
    routes: BTreeMap<String, u64>,
}

async fn one(
    http: &reqwest::Client,
    args: &Args,
    auth: &str,
    sql: String,
) -> Result<(usize, Option<String>)> {
    let r = http
        .post(format!("{}/v1/query", args.endpoint))
        .header(reqwest::header::AUTHORIZATION, auth)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&json!({ "sql": sql }))?)
        .send()
        .await?;
    let route = r
        .headers()
        .get("x-readyset-route")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    ensure!(r.status().is_success(), "http {}", r.status().as_u16());
    let mut body: Value = serde_json::from_slice(&r.bytes().await?)?;
    let mut rows = 0;
    loop {
        if let Some(e) = body.get("error").filter(|e| !e.is_null()) {
            anyhow::bail!("query error {}", e.get("code").unwrap_or(&Value::Null));
        }
        rows += body
            .get("data")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let Some(next) = body.get("next_uri").and_then(Value::as_str) else {
            break;
        };
        let last = next.ends_with("/final");
        let r = http
            .get(format!("{}{}", args.endpoint, next))
            .header(reqwest::header::AUTHORIZATION, auth)
            .send()
            .await?;
        ensure!(
            r.status().is_success(),
            "http {} on page",
            r.status().as_u16()
        );
        body = serde_json::from_slice(&r.bytes().await?)?;
        if last {
            break;
        }
    }
    Ok((rows, route))
}

pub async fn run(args: Args) -> Result<()> {
    ensure!(args.concurrency > 0 && args.key_space > 0, "bad arguments");
    let password = std::env::var("DATABEND_PASSWORD").unwrap_or_default();
    let auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{}:{password}", args.user))
    );
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(args.concurrency)
        .timeout(Duration::from_secs(60))
        .build()?;
    let counter = Arc::new(AtomicU64::new(args.seed.wrapping_mul(1 << 40)));
    let start = Instant::now();
    let measure_from = start + Duration::from_secs_f64(args.warmup);
    let deadline = measure_from + Duration::from_secs_f64(args.duration);
    let mut tasks = Vec::new();
    for _ in 0..args.concurrency {
        let (http, args, auth, counter) =
            (http.clone(), args.clone(), auth.clone(), counter.clone());
        tasks.push(tokio::spawn(async move {
            let mut out = Outcome::default();
            loop {
                let begin = Instant::now();
                if begin >= deadline {
                    break;
                }
                let sql = args
                    .template
                    .replace("{k}", &key(&args, counter.fetch_add(1, Ordering::Relaxed)));
                let result = one(&http, &args, &auth, sql).await;
                let end = Instant::now();
                if begin < measure_from || end > deadline {
                    continue;
                }
                match result {
                    Ok((rows, route)) => {
                        *out.routes
                            .entry(route.unwrap_or_else(|| "none".into()))
                            .or_default() += 1;
                        if rows == args.expect_rows {
                            out.lat_us.push(
                                u32::try_from(end.duration_since(begin).as_micros())
                                    .unwrap_or(u32::MAX),
                            );
                        } else {
                            *out.errors.entry(format!("rows {rows}")).or_default() += 1;
                        }
                    }
                    Err(e) => {
                        let mut s = e.to_string();
                        s.truncate(120);
                        *out.errors.entry(s).or_default() += 1;
                    }
                }
            }
            out
        }));
    }
    let mut all = Outcome::default();
    for t in tasks {
        let o = t.await.context("worker")?;
        all.lat_us.extend(o.lat_us);
        for (k, v) in o.errors {
            *all.errors.entry(k).or_default() += v;
        }
        for (k, v) in o.routes {
            *all.routes.entry(k).or_default() += v;
        }
    }
    all.lat_us.sort_unstable();
    let pct = |p: f64| -> Value {
        if all.lat_us.is_empty() {
            return Value::Null;
        }
        let i = ((all.lat_us.len() as f64 * p).ceil() as usize).clamp(1, all.lat_us.len()) - 1;
        json!(f64::from(all.lat_us[i]) / 1000.0)
    };
    let ok = all.lat_us.len() as u64;
    let failed: u64 = all.errors.values().sum();
    println!(
        "{}",
        json!({
            "label": args.label, "endpoint": args.endpoint, "kind": format!("{:?}", args.kind),
            "concurrency": args.concurrency, "duration_s": args.duration, "hot": args.hot,
            "key_space": args.key_space, "ok": ok, "failed": failed,
            "qps": (ok as f64 / args.duration * 10.0).round() / 10.0,
            "p50_ms": pct(0.50), "p90_ms": pct(0.90), "p99_ms": pct(0.99), "p999_ms": pct(0.999),
            "max_ms": all.lat_us.last().map(|v| f64::from(*v) / 1000.0),
            "mean_ms": if ok == 0 { Value::Null } else {
                json!(all.lat_us.iter().map(|v| u64::from(*v)).sum::<u64>() as f64 / ok as f64 / 1000.0)
            },
            "errors": all.errors, "routes": all.routes, "template": args.template,
        })
    );
    Ok(())
}
