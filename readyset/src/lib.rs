#![deny(macro_use_extern_crate)]

pub mod mysql;
pub mod psql;
mod query_logger;

use std::collections::HashMap;
use std::io;
use std::marker::Send;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, ensure};
use async_trait::async_trait;
use clap::{ArgGroup, Parser};
use database_utils::{DatabaseType, DatabaseURL};
use failpoint_macros::set_failpoint;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use health_reporter::{HealthReporter as AdapterHealthReporter, State as AdapterState};
use metrics_exporter_prometheus::PrometheusBuilder;
use nom_sql::Relation;
use readyset_adapter::backend::noria_connector::{NoriaConnector, ReadBehavior};
use readyset_adapter::backend::MigrationMode;
use readyset_adapter::fallback_cache::{
    DiskModeledCache, EvictionModeledCache, FallbackCache, SimpleFallbackCache,
};
use readyset_adapter::http_router::NoriaAdapterHttpRouter;
use readyset_adapter::migration_handler::MigrationHandler;
use readyset_adapter::proxied_queries_reporter::ProxiedQueriesReporter;
use readyset_adapter::query_status_cache::{MigrationStyle, QueryStatusCache};
use readyset_adapter::views_synchronizer::ViewsSynchronizer;
use readyset_adapter::{Backend, BackendBuilder, QueryHandler, UpstreamDatabase};
use readyset_client::consensus::{AuthorityControl, AuthorityType, ConsulAuthority};
#[cfg(feature = "failure_injection")]
use readyset_client::failpoints;
use readyset_client::metrics::recorded;
use readyset_client::{ReadySetError, ReadySetHandle, ViewCreateRequest};
use readyset_dataflow::Readers;
use readyset_server::metrics::{CompositeMetricsRecorder, MetricsRecorder};
use readyset_server::worker::readers::{retry_misses, Ack, BlockingRead, ReadRequestHandler};
use readyset_telemetry_reporter::{TelemetryBuilder, TelemetryEvent, TelemetryInitializer};
use readyset_tracing::{debug, error, info, warn};
use readyset_util::futures::abort_on_panic;
use readyset_util::redacted::RedactedString;
use readyset_version::*;
use stream_cancel::Valve;
use tokio::net;
use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{debug_span, span, Level};
use tracing_futures::Instrument;

// How frequently to try to establish an http registration for the first time or if the last tick
// failed and we need to establish a new one
const REGISTER_HTTP_INIT_INTERVAL: Duration = Duration::from_secs(2);

// How frequently to try to establish an http registration if we have one already
const REGISTER_HTTP_INTERVAL: Duration = Duration::from_secs(20);

const AWS_PRIVATE_IP_ENDPOINT: &str = "http://169.254.169.254/latest/meta-data/local-ipv4";
const AWS_METADATA_TOKEN_ENDPOINT: &str = "http://169.254.169.254/latest/api/token";

/// Timeout to use when connecting to the upstream database
const UPSTREAM_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[async_trait]
pub trait ConnectionHandler {
    type UpstreamDatabase: UpstreamDatabase;
    type Handler: QueryHandler;

    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: Backend<Self::UpstreamDatabase, Self::Handler>,
    );

    /// Return an immediate error to a newly-established connection, then immediately disconnect
    async fn immediate_error(self, stream: net::TcpStream, error_message: String);
}

/// How to behave when receiving unsupported `SET` statements.
///
/// Corresponds to the variants of [`noria_client::backend::UnsupportedSetMode`] that are exposed to
/// the user.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum UnsupportedSetMode {
    /// Return an error to the client (the default)
    Error,
    /// Proxy all subsequent statements to the upstream
    Proxy,
}

impl Default for UnsupportedSetMode {
    fn default() -> Self {
        Self::Error
    }
}

impl FromStr for UnsupportedSetMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "error" => Ok(Self::Error),
            "proxy" => Ok(Self::Proxy),
            _ => bail!(
                "Invalid value for unsupoported_set_mode; expected one of \"error\" or \"proxy\""
            ),
        }
    }
}

impl From<UnsupportedSetMode> for readyset_adapter::backend::UnsupportedSetMode {
    fn from(mode: UnsupportedSetMode) -> Self {
        match mode {
            UnsupportedSetMode::Error => Self::Error,
            UnsupportedSetMode::Proxy => Self::Proxy,
        }
    }
}

pub struct NoriaAdapter<H>
where
    H: ConnectionHandler,
{
    pub description: &'static str,
    pub default_address: SocketAddr,
    pub connection_handler: H,
    pub database_type: DatabaseType,
    /// SQL dialect to use when parsing queries
    pub parse_dialect: nom_sql::Dialect,
    /// Expression evaluation dialect to pass to ReadySet for all migration requests
    pub expr_dialect: readyset_data::Dialect,
}

#[derive(Parser, Debug)]
#[clap(group(
    ArgGroup::new("metrics")
        .multiple(true)
        .args(&["prometheus-metrics", "noria-metrics"]),
), version = VERSION_STR_PRETTY)]
pub struct Options {
    /// IP:PORT to listen on
    #[clap(long, short = 'a', env = "LISTEN_ADDRESS", parse(try_from_str))]
    address: Option<SocketAddr>,

    /// ReadySet deployment ID to attach to
    #[clap(long, env = "DEPLOYMENT", forbid_empty_values = true)]
    deployment: String,

    /// Database engine protocol to emulate
    #[clap(long, env = "DATABASE_TYPE", possible_values=&["mysql", "postgresql"])]
    pub database_type: DatabaseType,

    /// The authority to use. Possible values: zookeeper, consul, standalone.
    #[clap(
        long,
        env = "AUTHORITY",
        default_value_if("standalone", None, Some("standalone")),
        default_value = "consul",
        possible_values = &["consul", "zookeeper", "standalone"]
    )]
    authority: AuthorityType,

    /// Authority uri
    // NOTE: `authority_address` should come after `authority` for clap to set default values
    // properly
    #[clap(
        long,
        env = "AUTHORITY_ADDRESS",
        default_value_if("authority", Some("standalone"), Some(".")),
        default_value_if("authority", Some("consul"), Some("127.0.0.1:8500")),
        default_value_if("authority", Some("zookeeper"), Some("127.0.0.1:2181"))
    )]
    authority_address: String,

    /// Log slow queries (> 5ms)
    #[clap(long, hide = true)]
    log_slow: bool,

    /// Don't require authentication for any client connections
    #[clap(long, env = "ALLOW_UNAUTHENTICATED_CONNECTIONS")]
    allow_unauthenticated_connections: bool,

    /// Specify the migration mode for ReadySet to use
    #[clap(
        long,
        env = "QUERY_CACHING",
        default_value = "async",
        possible_values = &["inrequestpath", "explicit", "async"]
    )]
    query_caching: MigrationStyle,

    /// Sets the maximum time in minutes that we will retry migrations for in the
    /// migration handler. If this time is reached, the query will be exclusively
    /// sent to the upstream database.
    ///
    /// Defaults to 15 minutes.
    #[clap(
        long,
        env = "MAX_PROCESSING_MINUTES",
        default_value = "15",
        hide = true
    )]
    max_processing_minutes: u64,

    /// Sets the migration handlers's loop interval in milliseconds.
    #[clap(long, env = "MIGRATION_TASK_INTERVAL", default_value = "20000")]
    migration_task_interval: u64,

    /// Validate queries executing against noria with the upstream db.
    #[clap(
        long,
        env = "VALIDATE_QUERIES",
        requires("upstream-db-url"),
        hide = true
    )]
    validate_queries: bool,

    /// IP:PORT to host endpoint for scraping metrics from the adapter.
    #[clap(
        long,
        env = "METRICS_ADDRESS",
        default_value = "0.0.0.0:6034",
        parse(try_from_str)
    )]
    metrics_address: SocketAddr,

    /// Allow database connections authenticated as this user. Defaults to the username in
    /// --upstream-db-url if not set. Ignored if --allow-unauthenticated-connections is passed
    #[clap(long, env = "ALLOWED_USERNAME", short = 'u')]
    username: Option<String>,

    /// Password to authenticate database connections with. Defaults to the password in
    /// --upstream-db-url if not set. Ignored if --allow-unauthenticated-connections is passed
    #[clap(long, env = "ALLOWED_PASSWORD", short = 'p')]
    password: Option<RedactedString>,

    /// Enable recording and exposing Prometheus metrics
    #[clap(long, env = "PROMETHEUS_METRICS")]
    prometheus_metrics: bool,

    #[clap(long, hide = true)]
    noria_metrics: bool,

    /// Enable logging queries and execution metrics. This creates a histogram per unique query.
    #[clap(long, env = "QUERY_LOG", requires = "metrics")]
    query_log: bool,

    /// Enables logging ad-hoc queries in the query log. Useful for testing.
    #[clap(long, hide = true, env = "QUERY_LOG_AD_HOC", requires = "query-log")]
    query_log_ad_hoc: bool,

    /// Use the AWS EC2 metadata service to determine the external address of this noria adapter's
    /// http endpoint.
    #[clap(long)]
    use_aws_external_address: bool,

    #[clap(flatten)]
    tracing: readyset_tracing::Options,

    /// Test feature to fail invalidated queries in the serving path instead of going
    /// to fallback.
    #[clap(long, hide = true)]
    fail_invalidated_queries: bool,

    /// Allow executing, but ignore, unsupported `SET` statements.
    ///
    /// Takes precedence over any value passed to `--unsupported-set-mode`
    #[clap(long, hide = true, env = "ALLOW_UNSUPPORTED_SET")]
    allow_unsupported_set: bool,

    /// Configure how ReadySet behaves when receiving unsupported SET statements.
    ///
    /// The possible values are:
    ///
    /// * "error" (default) - return an error to the client
    /// * "proxy" - proxy all subsequent statements
    // NOTE: In order to keep `allow_unsupported_set` hidden, we're keeping these two flags separate
    // and *not* marking them as conflicting with each other.
    #[clap(
        long,
        env = "UNSUPPORTED_SET_MODE",
        default_value = "error",
        possible_values = &["error", "proxy"],
        parse(try_from_str)
    )]
    unsupported_set_mode: UnsupportedSetMode,

    // TODO(DAN): require explicit migrations
    /// Specifies the polling interval in seconds for requesting views from the Leader.
    #[clap(long, env = "OUTPUTS_POLLING_INTERVAL", default_value = "300")]
    views_polling_interval: u64,

    /// The time to wait before canceling a migration request. Defaults to 30 minutes.
    #[clap(
        long,
        hide = true,
        env = "MIGRATION_REQUEST_TIMEOUT",
        default_value = "1800000"
    )]
    migration_request_timeout_ms: u64,

    /// The time to wait before canceling a controller request. Defaults to 5 seconds.
    #[clap(long, hide = true, env = "CONTROLLER_TIMEOUT", default_value = "5000")]
    controller_request_timeout_ms: u64,

    /// Specifies the maximum continuous failure time for any given query, in seconds, before
    /// entering into a fallback recovery mode.
    #[clap(
        long,
        hide = true,
        env = "QUERY_MAX_FAILURE_SECONDS",
        default_value = "9223372036854775"
    )]
    query_max_failure_seconds: u64,

    /// Specifies the recovery period in seconds that we enter if a given query fails for the
    /// period of time designated by the query_max_failure_seconds flag.
    #[clap(
        long,
        hide = true,
        env = "FALLBACK_RECOVERY_SECONDS",
        default_value = "0"
    )]
    fallback_recovery_seconds: u64,

    /// Whether to use non-blocking or blocking reads against the cache.
    #[clap(long, env = "NON_BLOCKING_READS")]
    non_blocking_reads: bool,

    /// Run ReadySet in standalone mode, running a readyset-server instance within this adapter.
    #[clap(long, env = "STANDALONE", conflicts_with = "embedded-readers")]
    standalone: bool,

    /// Run ReadySet in embedded readers mode, running reader replicas (and only reader replicas)
    /// in the same process as the adapter
    ///
    /// Should be combined with passing `--no-readers` and `--reader-replicas` with the number of
    /// adapter instances to each server process.
    #[clap(long, env = "EMBEDDED_READERS", conflicts_with = "standalone")]
    embedded_readers: bool,

    #[clap(flatten)]
    server_worker_options: readyset_server::WorkerOptions,

    /// Whether to disable telemetry reporting. Defaults to false.
    #[clap(long, env = "DISABLE_TELEMETRY")]
    disable_telemetry: bool,

    /// Whether we should wait for a failpoint request to the adapters http router, which may
    /// impact startup.
    #[clap(long, hide = true)]
    wait_for_failpoint: bool,

    // TODO: This feature in general needs to be fleshed out significantly more. Off by default for
    // now.
    #[clap(flatten)]
    fallback_cache_options: FallbackCacheOptions,
}

// Command-line options for running the experimental fallback_cache.
//
// This option struct is intended to be embedded inside of a larger option struct using
// `#[clap(flatten)]`.
#[allow(missing_docs)] // Allows us to exclude docs (from doc comments) from --help text
#[derive(Parser, Debug)]
pub struct FallbackCacheOptions {
    /// Used to enable the fallback cache, which can handle serving all queries that we can't parse
    /// or support from an in-memory cache that lives in the adapter.
    #[clap(long, hide = true)]
    enable_fallback_cache: bool,

    /// Specifies a ttl in seconds for queries cached using the fallback cache.
    #[clap(long, hide = true, default_value = "120")]
    ttl_seconds: u64,

    /// If enabled, will model running the fallback cache off spinning disk.
    #[clap(long, hide = true)]
    model_disk: bool,

    #[clap(flatten)]
    eviction_options: FallbackCacheEvictionOptions,
}

// TODO:
// Change this to an enum that allows for a probabilistic strategy
//
// enum FallbackCacheEvictionStrategy {
// /// Don't model eviction
// None,
// /// This Cl's strategy
// Rate(f64),
// /// probabilistic strategy
// Random(f64)
// }
//
// Command-line options for running the experimental fallback_cache with eviction modeling.
//
// This option struct is intended to be embedded inside of a larger option struct using
// `#[clap(flatten)]`.
#[allow(missing_docs)] // Allows us to exclude docs (from doc comments) from --help text
#[derive(Parser, Debug)]
pub struct FallbackCacheEvictionOptions {
    /// If enabled, will model running the fallback cache with eviction.
    #[clap(long, hide = true)]
    model_eviction: bool,

    /// Provides a rate at which we will randomly evict queries.
    #[clap(long, hide = true, default_value = "0.01")]
    eviction_rate: f64,
}

impl<H> NoriaAdapter<H>
where
    H: ConnectionHandler + Clone + Send + Sync + 'static,
{
    pub fn run(&mut self, options: Options) -> anyhow::Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async { options.tracing.init("adapter", options.deployment.as_ref()) })?;
        info!(?options, "Starting ReadySet adapter");

        let upstream_config = options.server_worker_options.replicator_config.clone();
        let mut parsed_upstream_url = None;

        let users: &'static HashMap<String, String> =
            Box::leak(Box::new(if !options.allow_unauthenticated_connections {
                HashMap::from([(
                    options
                        .username
                        .or_else(|| {
                            // Default to the username in the upstream_db_url, if it's set and
                            // parseable
                            parsed_upstream_url
                                .get_or_insert_with(|| {
                                    upstream_config
                                        .upstream_db_url
                                        .as_ref()?
                                        .parse::<DatabaseURL>()
                                        .ok()
                                })
                                .as_ref()?
                                .user()
                                .map(ToOwned::to_owned)
                        })
                        .ok_or_else(|| {
                            anyhow!(
                                "Must specify --username/-u if one of \
                                 --allow-unauthenticated-connections or --upstream-db-url is not \
                                 passed"
                            )
                        })?,
                    options
                        .password
                        .map(|x| x.0)
                        .or_else(|| {
                            // Default to the password in the upstream_db_url, if it's set and
                            // parseable
                            parsed_upstream_url
                                .get_or_insert_with(|| {
                                    upstream_config
                                        .upstream_db_url
                                        .as_ref()?
                                        .parse::<DatabaseURL>()
                                        .ok()
                                })
                                .as_ref()?
                                .password()
                                .map(ToOwned::to_owned)
                        })
                        .ok_or_else(|| {
                            anyhow!(
                                "Must specify --password/-p if one of \
                                 --allow-unauthenticated-connections or --upstream-db-url is not \
                                 passed"
                            )
                        })?,
                )])
            } else {
                HashMap::new()
            }));
        info!(version = %VERSION_STR_ONELINE);

        if options.allow_unsupported_set {
            warn!(
                "Running with --allow-unsupported-set can cause certain queries to return \
                 incorrect results"
            )
        }

        let listen_address = options.address.unwrap_or(self.default_address);
        let listener = rt.block_on(tokio::net::TcpListener::bind(&listen_address))?;

        info!(%listen_address, "Listening for new connections");

        let auto_increments: Arc<RwLock<HashMap<Relation, AtomicUsize>>> = Arc::default();
        let query_cache: Arc<RwLock<HashMap<ViewCreateRequest, Relation>>> = Arc::default();
        let mut health_reporter = AdapterHealthReporter::new();

        let rs_connect = span!(Level::INFO, "Connecting to RS server");
        rs_connect.in_scope(|| info!(%options.authority_address, %options.deployment));

        let authority = options.authority.clone();
        let authority_address = match authority {
            AuthorityType::Standalone => options
                .server_worker_options
                .db_dir
                .as_ref()
                .map(|path| {
                    path.clone()
                        .into_os_string()
                        .into_string()
                        .unwrap_or_else(|_| options.authority_address.clone())
                })
                .unwrap_or_else(|| options.authority_address.clone()),
            _ => options.authority_address.clone(),
        };
        let deployment = options.deployment.clone();
        let migration_request_timeout = options.migration_request_timeout_ms;
        let controller_request_timeout = options.controller_request_timeout_ms;
        let server_supports_pagination = options
            .server_worker_options
            .enable_experimental_topk_support
            && options
                .server_worker_options
                .enable_experimental_paginate_support;

        let rh = rt.block_on(async {
            let authority = authority
                .to_authority(&authority_address, &deployment)
                .await;

            Ok::<ReadySetHandle, ReadySetError>(
                ReadySetHandle::with_timeouts(
                    authority,
                    Some(Duration::from_millis(controller_request_timeout)),
                    Some(Duration::from_millis(migration_request_timeout)),
                )
                .instrument(rs_connect.clone())
                .await,
            )
        })?;

        rs_connect.in_scope(|| info!("ReadySetHandle created"));

        let ctrlc = tokio::signal::ctrl_c();
        let mut sigterm = {
            let _guard = rt.enter();
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap()
        };
        let mut listener = Box::pin(futures_util::stream::select(
            TcpListenerStream::new(listener),
            futures_util::stream::select(
                ctrlc
                    .map(|r| {
                        r?;
                        Err(io::Error::new(io::ErrorKind::Interrupted, "got ctrl-c"))
                    })
                    .into_stream(),
                sigterm
                    .recv()
                    .map(futures_util::stream::iter)
                    .into_stream()
                    .flatten()
                    .map(|_| Err(io::Error::new(io::ErrorKind::Interrupted, "got SIGTERM"))),
            ),
        ));
        rs_connect.in_scope(|| info!("Now capturing ctrl-c and SIGTERM events"));

        let mut recorders = Vec::new();
        let prometheus_handle = if options.prometheus_metrics {
            let _guard = rt.enter();
            let database_label = match self.database_type {
                DatabaseType::MySQL => readyset_client_metrics::DatabaseType::MySql,
                DatabaseType::PostgreSQL => readyset_client_metrics::DatabaseType::Psql,
            };

            let recorder = PrometheusBuilder::new()
                .add_global_label("upstream_db_type", database_label)
                .add_global_label("deployment", &options.deployment)
                .build_recorder();

            let handle = recorder.handle();
            recorders.push(MetricsRecorder::Prometheus(recorder));
            Some(handle)
        } else {
            None
        };

        if options.noria_metrics {
            recorders.push(MetricsRecorder::Noria(
                readyset_server::NoriaMetricsRecorder::new(),
            ));
        }

        if !recorders.is_empty() {
            readyset_server::metrics::install_global_recorder(
                CompositeMetricsRecorder::with_recorders(recorders),
            )?;
        }

        rs_connect.in_scope(|| info!("PrometheusHandle created"));

        metrics::gauge!(
            recorded::READYSET_ADAPTER_VERSION,
            1.0,
            &[
                ("release_version", READYSET_VERSION.release_version),
                ("commit_id", READYSET_VERSION.commit_id),
                ("platform", READYSET_VERSION.platform),
                ("rustc_version", READYSET_VERSION.rustc_version),
                ("profile", READYSET_VERSION.profile),
                ("profile", READYSET_VERSION.profile),
                ("opt_level", READYSET_VERSION.opt_level),
            ]
        );
        metrics::counter!(
            recorded::NORIA_STARTUP_TIMESTAMP,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        );

        let (shutdown_sender, shutdown_recv) = tokio::sync::broadcast::channel(1);

        // Gate query log code path on the log flag existing.
        let qlog_sender = if options.query_log {
            rs_connect.in_scope(|| info!("Query logs are enabled. Spawning query logger"));
            let (qlog_sender, qlog_receiver) = tokio::sync::mpsc::unbounded_channel();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .max_blocking_threads(1)
                .build()
                .unwrap();

            // Spawn the actual thread to run the logger
            std::thread::Builder::new()
                .name("Query logger".to_string())
                .stack_size(2 * 1024 * 1024) // Use the same value tokio is using
                .spawn(move || {
                    runtime.block_on(query_logger::QueryLogger::run(qlog_receiver, shutdown_recv));
                    runtime.shutdown_background();
                })?;

            Some(qlog_sender)
        } else {
            rs_connect.in_scope(|| info!("Query logs are disabled"));
            None
        };

        let noria_read_behavior = if options.non_blocking_reads {
            rs_connect.in_scope(|| info!("Will perform NonBlocking Reads"));
            ReadBehavior::NonBlocking
        } else {
            rs_connect.in_scope(|| info!("Will perform Blocking Reads"));
            ReadBehavior::Blocking
        };

        let migration_style = options.query_caching;

        rs_connect.in_scope(|| info!(?migration_style));

        let query_status_cache: &'static _ =
            Box::leak(Box::new(QueryStatusCache::with_style(migration_style)));

        let telemetry_sender = rt.block_on(async {
            let proxied_queries_reporter =
                Arc::new(ProxiedQueriesReporter::new(query_status_cache));
            TelemetryInitializer::init(
                options.disable_telemetry,
                std::env::var("RS_API_KEY").ok(),
                vec![proxied_queries_reporter],
                options.deployment.clone(),
            )
            .await
        });

        let _ = telemetry_sender
            .send_event_with_payload(
                TelemetryEvent::AdapterStart,
                TelemetryBuilder::new()
                    .adapter_version(option_env!("CARGO_PKG_VERSION").unwrap_or_default())
                    .db_backend(format!("{:?}", &self.database_type).to_lowercase())
                    .build(),
            )
            .map_err(|error| warn!(%error, "Failed to initialize telemetry sender"));

        let migration_mode = match migration_style {
            MigrationStyle::Async | MigrationStyle::Explicit => MigrationMode::OutOfBand,
            MigrationStyle::InRequestPath => MigrationMode::InRequestPath,
        };

        rs_connect.in_scope(|| info!(?migration_mode));

        // Spawn a task for handling this adapter's HTTP request server.
        // This step is done as the last thing before accepting connections because it is used as
        // the health check for the service.
        let router_handle = {
            rs_connect.in_scope(|| info!("Spawning HTTP request server task"));
            let (handle, valve) = Valve::new();
            let (tx, rx) = if options.wait_for_failpoint {
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                (Some(Arc::new(tx)), Some(rx))
            } else {
                (None, None)
            };
            let http_server = NoriaAdapterHttpRouter {
                listen_addr: options.metrics_address,
                query_cache: query_status_cache,
                valve,
                prometheus_handle,
                health_reporter: health_reporter.clone(),
                failpoint_channel: tx,
            };

            let fut = async move {
                let http_listener = http_server.create_listener().await.unwrap();
                NoriaAdapterHttpRouter::route_requests(http_server, http_listener).await
            };

            rt.handle().spawn(fut);

            // If we previously setup a failpoint channel because wait_for_failpoint was enabled,
            // then we should wait to hear from the http router that a failpoint request was
            // handled.
            if let Some(mut rx) = rx {
                let fut = async move {
                    let _ = rx.recv().await;
                };
                rt.block_on(fut);
            }

            handle
        };

        let fallback_cache: Option<
            FallbackCache<
                <<H as ConnectionHandler>::UpstreamDatabase as UpstreamDatabase>::CachedReadResult,
            >,
        > = if cfg!(feature = "fallback_cache")
            && options.fallback_cache_options.enable_fallback_cache
        {
            let cache = if options.fallback_cache_options.model_disk {
                DiskModeledCache::new(Duration::new(options.fallback_cache_options.ttl_seconds, 0))
                    .into()
            } else if options
                .fallback_cache_options
                .eviction_options
                .model_eviction
            {
                EvictionModeledCache::new(
                    Duration::new(options.fallback_cache_options.ttl_seconds, 0),
                    options
                        .fallback_cache_options
                        .eviction_options
                        .eviction_rate,
                )
                .into()
            } else {
                SimpleFallbackCache::new(Duration::new(
                    options.fallback_cache_options.ttl_seconds,
                    0,
                ))
                .into()
            };
            Some(cache)
        } else {
            None
        };

        if let MigrationMode::OutOfBand = migration_mode {
            set_failpoint!("adapter-out-of-band");
            let rh = rh.clone();
            let (auto_increments, query_cache) = (auto_increments.clone(), query_cache.clone());
            let shutdown_recv = shutdown_sender.subscribe();
            let loop_interval = options.migration_task_interval;
            let max_retry = options.max_processing_minutes;
            let validate_queries = options.validate_queries;
            let dry_run = matches!(migration_style, MigrationStyle::Explicit);
            let upstream_config = options.server_worker_options.replicator_config.clone();
            let expr_dialect = self.expr_dialect;
            let fallback_cache = fallback_cache.clone();

            rs_connect.in_scope(|| info!("Spawning migration handler task"));
            let fut = async move {
                let connection = span!(Level::INFO, "migration task upstream database connection");
                let mut upstream =
                    if upstream_config.upstream_db_url.is_some() && !dry_run {
                        Some(
                            H::UpstreamDatabase::connect(upstream_config, fallback_cache)
                                .instrument(connection.in_scope(|| {
                                    span!(Level::INFO, "Connecting to upstream database")
                                }))
                                .await
                                .unwrap(),
                        )
                    } else {
                        None
                    };

                let schema_search_path = if let Some(upstream) = &mut upstream {
                    // TODO(ENG-1710): figure out a better error handling story for this task
                    upstream.schema_search_path().await.unwrap()
                } else {
                    Default::default()
                };

                //TODO(DAN): allow compatibility with async and explicit migrations
                let noria =
                    NoriaConnector::new(
                        rh.clone(),
                        auto_increments.clone(),
                        query_cache.clone(),
                        noria_read_behavior,
                        expr_dialect,
                        schema_search_path,
                        server_supports_pagination,
                    )
                    .instrument(connection.in_scope(|| {
                        span!(Level::DEBUG, "Building migration task noria connector")
                    }))
                    .await;

                let controller_handle = dry_run.then(|| rh.clone());
                let mut migration_handler = MigrationHandler::new(
                    noria,
                    upstream,
                    controller_handle,
                    query_status_cache,
                    expr_dialect,
                    validate_queries,
                    std::time::Duration::from_millis(loop_interval),
                    std::time::Duration::from_secs(max_retry * 60),
                    shutdown_recv,
                );

                migration_handler.run().await.map_err(move |e| {
                    error!(error = %e, "Migration Handler failed, aborting the process due to service entering a degraded state");
                    std::process::abort()
                })
            };

            rt.handle().spawn(abort_on_panic(fut));
        }

        if matches!(migration_style, MigrationStyle::Explicit) {
            rs_connect.in_scope(|| info!("Spawning explicit migrations task"));
            let rh = rh.clone();
            let loop_interval = options.views_polling_interval;
            let shutdown_recv = shutdown_sender.subscribe();
            let expr_dialect = self.expr_dialect;
            let fut = async move {
                let mut views_synchronizer = ViewsSynchronizer::new(
                    rh,
                    query_status_cache,
                    std::time::Duration::from_secs(loop_interval),
                    expr_dialect,
                    shutdown_recv,
                );
                views_synchronizer.run().await
            };
            rt.handle().spawn(abort_on_panic(fut));
        }

        // Spin up async task that is in charge of creating a session with the authority,
        // regularly updating the heartbeat to keep the session live, and registering the adapters
        // http endpoint.
        // For now we only support registering adapters over consul.
        if let AuthorityType::Consul = options.authority {
            set_failpoint!(failpoints::AUTHORITY);
            rs_connect.in_scope(|| info!("Spawning Consul session task"));
            let connection = span!(Level::DEBUG, "consul_session", addr = ?authority_address);
            let fut = reconcile_endpoint_registration(
                authority_address.clone(),
                deployment,
                options.metrics_address.port(),
                options.use_aws_external_address,
            )
            .instrument(connection);
            rt.handle().spawn(fut);
        }

        // Create a set of readers on this adapter. This will allow servicing queries directly
        // from readers on the adapter rather than across a network hop.
        let readers: Readers = Arc::new(Mutex::new(Default::default()));

        // Run a readyset-server instance within this adapter.
        let internal_server_handle = if options.standalone || options.embedded_readers {
            let (handle, valve) = Valve::new();
            let authority = options.authority.clone();
            let deployment = options.deployment.clone();
            let mut builder = readyset_server::Builder::from_worker_options(
                options.server_worker_options,
                &options.deployment,
            );
            let r = readers.clone();

            if options.embedded_readers {
                builder.as_reader_only();
                builder.cannot_become_leader();
            }

            builder.set_telemetry_sender(telemetry_sender.clone());

            let server_handle = rt.block_on(async move {
                let authority = Arc::new(
                    authority
                        .to_authority(&authority_address, &deployment)
                        .await,
                );

                builder
                    .start_with_readers(
                        authority,
                        r,
                        SocketAddr::new(
                            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                            4000,
                        ),
                        valve,
                        handle,
                    )
                    .await
            })?;

            Some(server_handle)
        } else {
            None
        };

        health_reporter.set_state(AdapterState::Healthy);

        if internal_server_handle.is_none() {
            // Validate compatibility with the external readyset-server instance
            rt.block_on(async { check_server_version_compatibility(&mut rh.clone()).await })?;
        }

        rs_connect.in_scope(|| info!(supported = %server_supports_pagination));

        let expr_dialect = self.expr_dialect;
        while let Some(Ok(s)) = rt.block_on(listener.next()) {
            let connection = span!(Level::DEBUG, "connection", addr = ?s.peer_addr().unwrap());
            connection.in_scope(|| info!("Accepted new connection"));

            // bunch of stuff to move into the async block below
            let rh = rh.clone();
            let (auto_increments, query_cache) = (auto_increments.clone(), query_cache.clone());
            let mut connection_handler = self.connection_handler.clone();
            let backend_builder = BackendBuilder::new()
                .slowlog(options.log_slow)
                .users(users.clone())
                .require_authentication(!options.allow_unauthenticated_connections)
                .dialect(self.parse_dialect)
                .query_log(qlog_sender.clone(), options.query_log_ad_hoc)
                .validate_queries(options.validate_queries, options.fail_invalidated_queries)
                .unsupported_set_mode(if options.allow_unsupported_set {
                    readyset_adapter::backend::UnsupportedSetMode::Allow
                } else {
                    options.unsupported_set_mode.into()
                })
                .migration_mode(migration_mode)
                .query_max_failure_seconds(options.query_max_failure_seconds)
                .telemetry_sender(telemetry_sender.clone())
                .fallback_recovery_seconds(options.fallback_recovery_seconds);
            let telemetry_sender = telemetry_sender.clone();

            // Initialize the reader layer for the adapter.
            let r = (options.standalone || options.embedded_readers).then(|| {
                // Create a task that repeatedly polls BlockingRead's every `RETRY_TIMEOUT`.
                // When the `BlockingRead` completes, tell the future to resolve with ack.
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(BlockingRead, Ack)>();
                rt.handle().spawn(retry_misses(rx));
                ReadRequestHandler::new(readers.clone(), tx, Duration::from_secs(5))
            });

            let query_status_cache = query_status_cache;
            let upstream_config = upstream_config.clone();
            let fallback_cache = fallback_cache.clone();
            let fut = async move {
                let upstream_res = if upstream_config.upstream_db_url.is_some() {
                    set_failpoint!(failpoints::UPSTREAM);
                    timeout(
                        UPSTREAM_CONNECTION_TIMEOUT,
                        H::UpstreamDatabase::connect(upstream_config, fallback_cache),
                    )
                    .instrument(debug_span!("Connecting to upstream database"))
                    .await
                    .map_err(|_| "Connection timed out".to_owned())
                    .and_then(|r| r.map_err(|e| e.to_string()))
                    .map_err(|e| format!("Error connecting to upstream database: {}", e))
                    .map(Some)
                } else {
                    Ok(None)
                };

                match upstream_res {
                    Ok(mut upstream) => {
                        if let Err(e) =
                            telemetry_sender.send_event(TelemetryEvent::UpstreamConnected)
                        {
                            warn!(error = %e, "Failed to send upstream connected metric");
                        }

                        // Query the upstream for its currently-configured schema search path
                        //
                        // NOTE: when we start tracking all configuration parameters, this should be
                        // folded into whatever loads those initially
                        let schema_search_path_res = if let Some(upstream) = &mut upstream {
                            upstream.schema_search_path().await.map(|ssp| {
                                debug!(
                                    schema_search_path = ?ssp,
                                    "Setting initial schema search path for backend"
                                );
                                ssp
                            })
                        } else {
                            Ok(Default::default())
                        };

                        match schema_search_path_res {
                            Ok(ssp) => {
                                let noria = NoriaConnector::new_with_local_reads(
                                    rh.clone(),
                                    auto_increments.clone(),
                                    query_cache.clone(),
                                    noria_read_behavior,
                                    r,
                                    expr_dialect,
                                    ssp,
                                    server_supports_pagination,
                                )
                                .instrument(debug_span!("Building noria connector"))
                                .await;

                                let backend = backend_builder.clone().build(
                                    noria,
                                    upstream,
                                    query_status_cache,
                                );
                                connection_handler.process_connection(s, backend).await;
                            }
                            Err(error) => {
                                error!(
                                    %error,
                                    "Error loading initial schema search path from upstream"
                                );
                                connection_handler
                                    .immediate_error(
                                        s,
                                        format!(
                                            "Error loading initial schema search path from \
                                             upstream: {error}"
                                        ),
                                    )
                                    .await;
                            }
                        }
                    }
                    Err(error) => {
                        error!(%error, "Error during initial connection establishment");
                        connection_handler.immediate_error(s, error).await;
                    }
                }

                debug!("disconnected");
            }
            .instrument(connection);

            rt.handle().spawn(fut);
        }

        let rs_shutdown = span!(Level::INFO, "RS server Shutting down");
        health_reporter.set_state(AdapterState::ShuttingDown);
        // Dropping the sender acts as a shutdown signal.
        drop(shutdown_sender);

        rs_shutdown.in_scope(|| {
            info!("Shutting down all tcp streams started by the adapters http router")
        });
        drop(router_handle);

        rs_shutdown.in_scope(|| info!("Dropping controller handle"));
        drop(rh);

        // Send shutdown telemetry events
        if internal_server_handle.is_some() {
            let _ = telemetry_sender.send_event(TelemetryEvent::ServerStop);
        }

        let _ = telemetry_sender.send_event(TelemetryEvent::AdapterStop);
        rs_shutdown.in_scope(|| {
            info!("Waiting up to 5s for telemetry reporter to drain in-flight metrics")
        });
        rt.block_on(async move {
            match telemetry_sender
                .graceful_shutdown(std::time::Duration::from_secs(5))
                .await
            {
                Ok(_) => info!("TelemetrySender shutdown gracefully"),
                Err(e) => info!(error=%e, "TelemetrySender did not shut down gracefully"),
            }
        });

        // We use `shutdown_timeout` instead of `shutdown_background` in case any
        // blocking IO is ongoing.
        rs_shutdown.in_scope(|| info!("Waiting up to 20s for tasks to complete shutdown"));
        rt.shutdown_timeout(std::time::Duration::from_secs(20));
        rs_shutdown.in_scope(|| info!("Shutdown completed successfully"));

        Ok(())
    }
}

async fn check_server_version_compatibility(rh: &mut ReadySetHandle) -> anyhow::Result<()> {
    let server_version = rh.version().await?;
    debug!(server_version);
    ensure!(
        RELEASE_VERSION == server_version,
        "Adapter and server version mismatch. Expected {} found {}",
        RELEASE_VERSION,
        server_version
    );
    Ok(())
}

async fn my_ip(destination: &str, use_aws_external: bool) -> Option<IpAddr> {
    if use_aws_external {
        return my_aws_ip().await.ok();
    }

    let socket = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(_) => return None,
    };

    match socket.connect(destination).await {
        Ok(()) => (),
        Err(_) => return None,
    };

    match socket.local_addr() {
        Ok(addr) => Some(addr.ip()),
        Err(_) => None,
    }
}

// TODO(peter): Pull this out to a shared util between readyset-server and readyset-adapter
async fn my_aws_ip() -> anyhow::Result<IpAddr> {
    let client = reqwest::Client::builder().build()?;
    let token: String = client
        .put(AWS_METADATA_TOKEN_ENDPOINT)
        .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
        .send()
        .await?
        .text()
        .await?
        .parse()?;

    Ok(client
        .get(AWS_PRIVATE_IP_ENDPOINT)
        .header("X-aws-ec2-metadata-token", &token)
        .send()
        .await?
        .text()
        .await?
        .parse()?)
}

/// Facilitates continuously updating consul with this adapters externally accessibly http
/// endpoint.
async fn reconcile_endpoint_registration(
    authority_address: String,
    deployment: String,
    port: u16,
    use_aws_external: bool,
) {
    let connect_string = format!("http://{}/{}", &authority_address, &deployment);
    debug!("{}", connect_string);
    let authority = ConsulAuthority::new(&connect_string).unwrap();

    let mut initializing = true;
    let mut interval = tokio::time::interval(REGISTER_HTTP_INIT_INTERVAL);
    let mut session_id = None;

    async fn needs_refresh(id: &Option<String>, consul: &ConsulAuthority) -> bool {
        if let Some(id) = id {
            consul.worker_heartbeat(id.to_owned()).await.is_err()
        } else {
            true
        }
    }

    loop {
        interval.tick().await;
        debug!("Checking authority registry");

        if needs_refresh(&session_id, &authority).await {
            // If we fail this heartbeat, we assume we need to create a new session.
            if let Err(e) = authority.init().await {
                error!(%e, "encountered error while trying to initialize authority in readyset-adapter");
                // Try again on next tick, and reduce the polling interval until a new session is
                // established.
                initializing = true;
                continue;
            }
        }

        // We try to update our http endpoint every iteration regardless because it may
        // have changed.
        let ip = match my_ip(&authority_address, use_aws_external).await {
            Some(ip) => ip,
            None => {
                info!("Failed to retrieve IP. Will try again on next tick");
                continue;
            }
        };
        let http_endpoint = SocketAddr::new(ip, port);

        match authority.register_adapter(http_endpoint).await {
            Ok(id) => {
                if initializing {
                    info!("Established authority connection, reducing polling interval");
                    // Switch to a longer polling interval after the first registration is made
                    interval = tokio::time::interval(REGISTER_HTTP_INTERVAL);
                    initializing = false;
                }

                session_id = id;
            }
            Err(e) => {
                error!(%e, "encountered error while trying to register adapter endpoint in authority")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Certain clap things, like `requires`, only ever throw an error at runtime, not at
    // compile-time - this tests that none of those happen
    #[test]
    fn arg_parsing_noria_standalone() {
        let opts = Options::parse_from(vec![
            "readyset",
            "--database-type",
            "mysql",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--authority-address",
            "zookeeper:2181",
            "--allow-unauthenticated-connections",
        ]);

        assert_eq!(opts.deployment, "test");
    }

    #[test]
    fn arg_parsing_with_upstream() {
        let opts = Options::parse_from(vec![
            "readyset",
            "--database-type",
            "mysql",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--authority-address",
            "zookeeper:2181",
            "--allow-unauthenticated-connections",
            "--upstream-db-url",
            "mysql://root:password@mysql:3306/readyset",
        ]);

        assert_eq!(opts.deployment, "test");
    }

    #[test]
    fn async_migrations_param_defaults() {
        let opts = Options::parse_from(vec![
            "readyset",
            "--database-type",
            "mysql",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--authority-address",
            "zookeeper:2181",
            "--allow-unauthenticated-connections",
            "--upstream-db-url",
            "mysql://root:password@mysql:3306/readyset",
            "--query-caching=async",
        ]);

        assert_eq!(opts.max_processing_minutes, 15);
        assert_eq!(opts.migration_task_interval, 20000);
    }
}
