use std::convert::TryFrom;
use std::future::Future;
use std::num::ParseIntError;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use mysql_async::prelude::Queryable;
use mysql_async::ServerError;
use readyset_client::status::{ReadySetStatus, SnapshotStatus};
use readyset_client::ReadySetResult;
use readyset_tracing::info;

pub mod generate;
pub mod multi_thread;
pub mod path;
pub mod prometheus;
pub mod query;
pub mod spec;

pub fn us_to_ms(us: u64) -> f64 {
    us as f64 / 1000.
}

pub fn seconds_as_str_to_duration(input: &str) -> std::result::Result<Duration, ParseIntError> {
    Ok(Duration::from_secs(u64::from_str(input)?))
}

pub async fn run_for(
    func: impl Future<Output = Result<()>>,
    duration: Option<Duration>,
) -> Result<()> {
    if let Some(duration) = duration {
        match tokio::time::timeout(duration, func).await {
            Ok(r) => r,
            Err(_) => Ok(()), //Timeout was hit without failing prior.
        }
    } else {
        func.await
    }
}

#[macro_export]
macro_rules! make_key {
    ($name: expr, $unit: ident) => {
        ::metrics::Key::from_name(format!(
            "benchmark.{}_{}",
            $name,
            ::metrics::Unit::$unit.as_str(),
        ))
    };
    ($name: expr, $unit: ident $(, $label_key: expr => $label_value: expr)*) => {{
        let labels = vec![$(($label_key, $label_value),)*]
            .iter()
            .map(::metrics::Label::from)
            .collect::<Vec<_>>();
        ::metrics::Key::from_parts(
            format!(
                "benchmark.{}_{}",
                $name,
                ::metrics::Unit::$unit.as_str(),
            ),
            labels
        )
    }};
}

/// Waits for the back-end to return that it is ready to process queries.
pub async fn readyset_ready(target: &str) -> ReadySetResult<()> {
    info!("Waiting for the target database to be ready...");
    let opts = mysql_async::Opts::from_url(target).unwrap();
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();

    loop {
        let res = conn.query("SHOW READYSET STATUS").await;
        if let Err(mysql_async::Error::Server(ServerError { code, .. })) = res {
            // If it is a syntax error, this is not a ReadySet adapter and does not support this
            // syntax.
            if code == 1064 {
                info!("Database ready!");
                break;
            }
        }

        let res: Vec<mysql_async::Row> = res?;
        let status = ReadySetStatus::try_from(res)?;
        if status.snapshot_status == SnapshotStatus::Completed {
            info!("Database ready!");
            break;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}

#[macro_export]
macro_rules! benchmark_gauge {
    ($name: expr, $unit: ident, $description: expr, $value: expr $(, $label_key: expr => $label_value: expr)*) => {
        if let Some(recorder) = metrics::try_recorder() {
            let key = $crate::make_key!($name, $unit $(, $label_key => $label_value)*);
            let g = recorder.register_gauge(&key);
            recorder.describe_gauge(key.into_parts().0, Some(::metrics::Unit::$unit), $description);
            g.set($value);
        }
    };
}

#[macro_export]
macro_rules! benchmark_counter {
    ($name: expr, $unit: ident, $description: expr, $value: expr $(, $label_key: expr => $label_value: expr)*) => {
        if let Some(recorder) = metrics::try_recorder() {
            let key = $crate::make_key!($name, $unit $(, $label_key => $label_value)*);
            let c = recorder.register_counter(&key);
            recorder.describe_counter(key.into_parts().0, Some(::metrics::Unit::$unit), $description);
            c.increment($value);
        }
    };
    ($name: expr, $unit: ident, $description: expr) => {
        benchmark_counter!($name, $unit, $description, 0)
    };
}

#[macro_export]
macro_rules! benchmark_increment_counter {
    ($name: expr, $unit: ident, $value: expr $(, $label_key: expr => $label_value: expr)*) => {
        if let Some(recorder) = metrics::try_recorder() {
            let key = $crate::make_key!($name, $unit $(, $label_key => $label_value)*);
            let c = recorder.register_counter(&key);
            c.increment($value);
        }
    };
    ($name: expr, $unit: ident) => {
        benchmark_increment_counter!($name, $unit, 1)
    };
}

#[macro_export]
macro_rules! benchmark_histogram {
    ($name: expr, $unit: ident, $description: expr, $value: expr $(, $label_key: expr => $label_value: expr)*) => {
        if let Some(recorder) = metrics::try_recorder() {
            let key = $crate::make_key!($name, $unit $(, $label_key => $label_value)*);
            let h = recorder.register_histogram(&key);
            recorder.describe_histogram(key.into_parts().0, Some(::metrics::Unit::$unit), $description);
            h.record($value);
        }
    };
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use metrics_exporter_prometheus::*;

    fn setup() -> PrometheusHandle {
        let recorder = Box::leak(Box::new({
            let builder =
                PrometheusBuilder::new().idle_timeout(metrics_util::MetricKindMask::ALL, None);
            builder.build_recorder()
        }));
        let handle = recorder.handle();
        metrics::set_recorder(recorder).unwrap();
        handle
    }

    #[test]
    fn test_metrics_macros() {
        let handle = setup();

        benchmark_gauge!("test", Count, "desc", 1.0);
        benchmark_gauge!("test", Count, "desc", 2.0);
        benchmark_gauge!("test", Count, "desc", 3.0, "a" => "b");
        benchmark_gauge!("test", Count, "desc", 4.0, "c" => "d");
        benchmark_gauge!("test", Count, "desc", 5.0, "e" => "f", "g" => "h");

        benchmark_counter!("one", Seconds, "desc");
        benchmark_counter!("two", Seconds, "desc", 39);
        benchmark_increment_counter!("two", Seconds, 2);
        benchmark_increment_counter!("two", Seconds);
        benchmark_increment_counter!("three", Seconds);

        for i in 1..=100 {
            benchmark_histogram!("percentile", Bytes, "desc", i as f64);
        }

        // TODO:  If https://github.com/metrics-rs/metrics/pull/236 lands, use that instead of checking rendered text
        let output = handle.render();
        println!("{}", output);
        assert!(output.contains(indoc! {"
            # TYPE benchmark_test_count gauge
            benchmark_test_count"
        }));
        assert!(output.contains("\nbenchmark_test_count{a=\"b\"} 3\n"));
        assert!(output.contains("\nbenchmark_test_count{c=\"d\"} 4\n"));
        assert!(
            output.contains("\nbenchmark_test_count{e=\"f\",g=\"h\"} 5\n")
                || output.contains("\nbenchmark_test_count{g=\"h\",e=\"f\"} 5\n")
        );
        assert!(output.contains(indoc! {"
            # TYPE benchmark_one_seconds counter
            benchmark_one_seconds 0
        "}));
        assert!(output.contains(indoc! {"
            # TYPE benchmark_two_seconds counter
            benchmark_two_seconds 42
        "}));
        assert!(output.contains(indoc! {"
            # TYPE benchmark_three_seconds counter
            benchmark_three_seconds 1
        "}));
        // TODO: We shouldn't hold test against these exact values. We should instead extract the
        // values and use `assert_relative_eq!()`.
        assert!(output.contains(indoc! {r#"
            # TYPE benchmark_percentile_bytes summary
            benchmark_percentile_bytes{quantile="0"} 1
            benchmark_percentile_bytes{quantile="0.5"} 50.00385027884824
            benchmark_percentile_bytes{quantile="0.9"} 90.00813093751373
            benchmark_percentile_bytes{quantile="0.95"} 95.00219629040446
            benchmark_percentile_bytes{quantile="0.99"} 98.99803587754256
            benchmark_percentile_bytes{quantile="0.999"} 99.9929826824495
            benchmark_percentile_bytes{quantile="1"} 100
            benchmark_percentile_bytes_sum 5050
            benchmark_percentile_bytes_count 100
        "#}));
    }
}
