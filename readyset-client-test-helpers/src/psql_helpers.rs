use std::env;

use async_trait::async_trait;
use readyset_adapter::backend::{QueryDestination, QueryInfo};
use readyset_adapter::Backend;
use readyset_psql::{PostgreSqlQueryHandler, PostgreSqlUpstream};
use readyset_tracing::error;
use tokio::net::TcpStream;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use crate::{sleep, Adapter};

pub fn upstream_config() -> tokio_postgres::Config {
    let mut config = tokio_postgres::Config::new();
    config
        .user(&env::var("PGUSER").unwrap_or_else(|_| "postgres".into()))
        .password(
            env::var("PGPASSWORD")
                .unwrap_or_else(|_| "noria".into())
                .as_bytes(),
        )
        .host(&env::var("PGHOST").unwrap_or_else(|_| "localhost".into()))
        .port(
            env::var("PGPORT")
                .unwrap_or_else(|_| "5432".into())
                .parse()
                .unwrap(),
        );
    config
}

pub struct PostgreSQLAdapter;
#[async_trait]
impl Adapter for PostgreSQLAdapter {
    type ConnectionOpts = tokio_postgres::Config;
    type Upstream = PostgreSqlUpstream;
    type Handler = PostgreSqlQueryHandler;

    const DIALECT: nom_sql::Dialect = nom_sql::Dialect::PostgreSQL;

    const EXPR_DIALECT: readyset_data::Dialect = readyset_data::Dialect::DEFAULT_POSTGRESQL;

    fn connection_opts_with_port(port: u16) -> Self::ConnectionOpts {
        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1").port(port).dbname("noria");
        config
    }

    fn url() -> String {
        format!(
            "postgresql://{}:{}@{}:{}/noria",
            env::var("PGUSER").unwrap_or_else(|_| "postgres".into()),
            env::var("PGPASSWORD").unwrap_or_else(|_| "noria".into()),
            env::var("PGHOST").unwrap_or_else(|_| "localhost".into()),
            env::var("PGPORT").unwrap_or_else(|_| "5432".into()),
        )
    }

    async fn recreate_database() {
        let mut config = upstream_config();

        let (client, connection) = config.dbname("postgres").connect(NoTls).await.unwrap();
        tokio::spawn(connection);
        while let Err(error) = client.simple_query("DROP DATABASE IF EXISTS noria").await {
            error!(%error, "Error dropping database");
            sleep().await
        }

        client.simple_query("CREATE DATABASE noria").await.unwrap();
    }

    async fn run_backend(backend: Backend<Self::Upstream, Self::Handler>, s: TcpStream) {
        psql_srv::run_backend(readyset_psql::Backend(backend), s).await
    }
}

/// Retrieves where the query executed by parsing the row returned by EXPLAIN LAST STATEMENT.
pub async fn last_query_info(conn: &Client) -> QueryInfo {
    let row = match conn
        .simple_query("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    {
        SimpleQueryMessage::Row(row) => row,
        _ => panic!("Unexpected SimpleQueryMessage"),
    };

    let destination = QueryDestination::try_from(row.get("Query_destination").unwrap()).unwrap();
    let noria_error = row.get("ReadySet_error").unwrap().to_owned();

    QueryInfo {
        destination,
        noria_error,
    }
}
