//! Load generators for Databend-compatible HTTP endpoints.
//!
//! Every workload talks the Databend HTTP query API (`POST /v1/query` + `next_uri` pages), so
//! the same run can target Databend directly or readyset-databend in front of it.
use anyhow::Result;
use clap::{Parser, Subcommand};

mod point;

#[derive(Parser)]
#[command(about = "Load generators for Databend HTTP endpoints")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Closed-loop point lookups: latency percentiles and throughput.
    Point(point::Args),
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Point(args) => point::run(args).await,
    }
}
