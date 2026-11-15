//! Notification health without destination, tenant, alert or credential labels.
use crate::self_metrics::SelfMetrics;
use std::{future::Future, time::Instant};

fn source(value: &str) -> &'static str {
    match value {
        "monitor" => "monitor",
        "slo" => "slo",
        "anomaly" => "anomaly",
        "detection" => "detection",
        "alert_rule" => "alert_rule",
        "test" => "test",
        _ => "other",
    }
}
fn channel_type(value: &str) -> &'static str {
    match value {
        "email" => "email",
        "slack" => "slack",
        "slack_app" => "slack_app",
        "webhook" => "webhook",
        "pagerduty" => "pagerduty",
        "rootly" => "rootly",
        "discord" => "discord",
        "alertmanager" => "alertmanager",
        _ => "other",
    }
}
struct DeliveryObservation<'a> {
    metrics: Option<&'a SelfMetrics>,
    source: &'static str,
    channel: &'static str,
    started: Instant,
    outcome: &'static str,
}
impl Drop for DeliveryObservation<'_> {
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics {
            let labels = [("source", self.source), ("channel_type", self.channel)];
            metrics.add_gauge("rush_notification_deliveries_inflight", &labels, -1.0);
            let labels = [
                ("source", self.source),
                ("channel_type", self.channel),
                ("outcome", self.outcome),
            ];
            metrics.inc_counter("rush_notification_deliveries_total", &labels, 1);
            metrics.observe_histogram(
                "rush_notification_delivery_duration_ms",
                &labels,
                self.started.elapsed().as_secs_f64() * 1000.0,
            );
        }
    }
}

pub async fn observe_delivery(
    metrics: Option<&SelfMetrics>,
    origin: &str,
    channel: &str,
    work: impl Future<Output = Result<(), String>>,
) -> Result<(), String> {
    let mut observation = DeliveryObservation {
        metrics,
        source: source(origin),
        channel: channel_type(channel),
        started: Instant::now(),
        outcome: "cancelled",
    };
    if let Some(metrics) = metrics {
        metrics.add_gauge(
            "rush_notification_deliveries_inflight",
            &[
                ("source", observation.source),
                ("channel_type", observation.channel),
            ],
            1.0,
        );
    }
    let result = work.await;
    observation.outcome = if result.is_ok() { "sent" } else { "error" };
    result
}

pub fn record_lookup(metrics: Option<&SelfMetrics>, origin: &str, outcome: &'static str) {
    if let Some(metrics) = metrics {
        let outcome = match outcome {
            "ready" | "skipped" | "error" => outcome,
            _ => "other",
        };
        metrics.inc_counter(
            "rush_notification_channel_lookups_total",
            &[("source", source(origin)), ("outcome", outcome)],
            1,
        );
    }
}
pub fn record_log_write(metrics: Option<&SelfMetrics>, origin: &str, ok: bool) {
    if let Some(metrics) = metrics {
        metrics.inc_counter(
            "rush_notification_log_writes_total",
            &[
                ("source", source(origin)),
                ("outcome", if ok { "ok" } else { "error" }),
            ],
            1,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    #[tokio::test]
    async fn delivery_outcomes_are_distinct_and_labels_never_include_error_or_destination() {
        let metrics = SelfMetrics::new();
        observe_delivery(Some(&metrics), "monitor", "email", async { Ok(()) })
            .await
            .unwrap();
        assert!(
            observe_delivery(
                Some(&metrics),
                "tenant-secret",
                "https://secret-destination",
                async { Err("credential-secret".into()) }
            )
            .await
            .is_err()
        );
        record_log_write(Some(&metrics), "monitor", false);
        let text = metrics.render_prometheus();
        assert!(text.contains("rush_notification_deliveries_total{channel_type=\"email\",outcome=\"sent\",source=\"monitor\"} 1"));
        assert!(text.contains("rush_notification_deliveries_total{channel_type=\"other\",outcome=\"error\",source=\"other\"} 1"));
        assert!(text.contains(
            "rush_notification_log_writes_total{outcome=\"error\",source=\"monitor\"} 1"
        ));
        assert!(!text.contains("secret"));
    }
    #[tokio::test]
    async fn cancelled_delivery_releases_inflight_without_recording_a_send() {
        let metrics = Arc::new(SelfMetrics::new());
        let registry = metrics.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            observe_delivery(Some(&registry), "slo", "webhook", async {
                tx.send(()).unwrap();
                std::future::pending::<Result<(), String>>().await
            })
            .await
        });
        rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let text = metrics.render_prometheus();
        assert!(text.contains(
            "rush_notification_deliveries_inflight{channel_type=\"webhook\",source=\"slo\"} 0"
        ));
        assert!(text.contains("rush_notification_deliveries_total{channel_type=\"webhook\",outcome=\"cancelled\",source=\"slo\"} 1"));
        assert!(!text.contains("outcome=\"sent\""));
    }
}
