//! This crate provides a reusable mechanism for reporting telemetry payloads to the
//! ReadySet Segment HTTP source endpoint.
//!
//! In the future, the plan is to extend this with support for things like background reporting,
//! more advanced API token validation, integration with `metrics`, etc.

mod error;
pub use error::*;

mod reporter;
pub use reporter::*;

mod sender;
pub use sender::*;

mod telemetry;
pub use telemetry::*;
use tokio::sync::mpsc::channel;
use tokio::sync::oneshot;

pub const TELMETRY_CHANNEL_LEN: usize = 1024;

pub struct TelemetryInitializer {}

impl TelemetryInitializer {
    /// Initializes a background task and returns a TelemetrySender handle
    pub async fn init(
        disable_telemetry: bool,
        api_key: Option<String>,
        periodic_reporters: Vec<PeriodicReporter>,
        deployment_id: String,
    ) -> TelemetrySender {
        if disable_telemetry {
            return TelemetrySender::new_no_op();
        }
        let (tx, rx) = channel(TELMETRY_CHANNEL_LEN); // Arbitrary number of metrics to allow in queue before dropping them
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (shutdown_ack_tx, shutdown_ack_rx) = oneshot::channel();
        let sender = TelemetrySender::new(tx, shutdown_tx, shutdown_ack_rx);

        tokio::spawn(async move {
            let mut telemetry_reporter =
                TelemetryReporter::new(rx, api_key, shutdown_rx, shutdown_ack_tx, deployment_id);
            for reporter in periodic_reporters {
                telemetry_reporter
                    .register_periodic_reporter(reporter)
                    .await;
            }
            telemetry_reporter.run().await;
        });

        sender
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn test_init() -> (TelemetrySender, TelemetryReporter) {
        readyset_tracing::init_test_logging();
        let (tx, rx) = channel(TELMETRY_CHANNEL_LEN); // Arbitrary number of metrics to allow in queue before dropping them
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (shutdown_ack_tx, shutdown_ack_rx) = oneshot::channel();
        let sender = TelemetrySender::new(tx, shutdown_tx, shutdown_ack_rx);

        let reporter = TelemetryReporter::new(
            rx,
            Some("api-key".into()),
            shutdown_rx,
            shutdown_ack_tx,
            "deployment_id".into(),
        );

        (sender, reporter)
    }
}
