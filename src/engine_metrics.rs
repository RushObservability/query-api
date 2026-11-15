//! Bounded evaluation outcomes, including cancelled work and successful no-data reads.
use crate::self_metrics::SelfMetrics;
use std::time::Instant;

#[derive(Clone, Copy)]
pub enum EvaluationOutcome {
    Completed,
    NoData,
    Error,
    AdmissionRejected,
    InvalidConfig,
}

pub struct EvaluationObservation<'a> {
    metrics: Option<&'a SelfMetrics>,
    engine: &'static str,
    started: Instant,
    outcome: &'static str,
}

impl<'a> EvaluationObservation<'a> {
    pub fn new(metrics: Option<&'a SelfMetrics>, engine: &'static str) -> Self {
        let engine = match engine {
            "monitor_engine" | "siem_engine" | "slo_engine" | "anomaly_engine" => engine,
            _ => "other",
        };
        if let Some(metrics) = metrics {
            metrics.add_gauge(
                "rush_engine_evaluations_inflight",
                &[("engine", engine)],
                1.0,
            );
        }
        Self {
            metrics,
            engine,
            started: Instant::now(),
            outcome: "cancelled",
        }
    }
    pub fn finish(&mut self, outcome: EvaluationOutcome) {
        self.outcome = match outcome {
            EvaluationOutcome::Completed => "ok",
            EvaluationOutcome::NoData => "no_data",
            EvaluationOutcome::Error => "error",
            EvaluationOutcome::AdmissionRejected => "admission_rejected",
            EvaluationOutcome::InvalidConfig => "invalid_config",
        };
    }
}

impl Drop for EvaluationObservation<'_> {
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics {
            let labels = [("engine", self.engine), ("outcome", self.outcome)];
            metrics.add_gauge(
                "rush_engine_evaluations_inflight",
                &[("engine", self.engine)],
                -1.0,
            );
            metrics.inc_counter("rush_engine_evaluations_total", &labels, 1);
            metrics.observe_histogram(
                "rush_engine_evaluation_duration_ms",
                &labels,
                self.started.elapsed().as_secs_f64() * 1000.0,
            );
            if matches!(self.outcome, "ok" | "no_data") {
                metrics.set_gauge(
                    "rush_engine_evaluation_last_success_timestamp_seconds",
                    &[("engine", self.engine)],
                    chrono::Utc::now().timestamp() as f64,
                );
            }
        }
    }
}

pub fn due(metrics: Option<&SelfMetrics>, engine: &'static str, count: usize) {
    if let Some(metrics) = metrics {
        metrics.set_gauge(
            "rush_engine_evaluations_due",
            &[("engine", engine)],
            count as f64,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    #[tokio::test]
    async fn cancellation_releases_inflight_and_is_not_a_success() {
        let metrics = Arc::new(SelfMetrics::new());
        let registry = metrics.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _observation = EvaluationObservation::new(Some(&registry), "monitor_engine");
            tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let text = metrics.render_prometheus();
        assert!(text.contains("rush_engine_evaluations_inflight{engine=\"monitor_engine\"} 0"));
        assert!(text.contains(
            "rush_engine_evaluations_total{engine=\"monitor_engine\",outcome=\"cancelled\"} 1"
        ));
        assert!(!text.contains("last_success"));
    }
    #[test]
    fn failed_work_does_not_advance_last_success_and_labels_are_bounded() {
        let metrics = SelfMetrics::new();
        {
            let mut work = EvaluationObservation::new(Some(&metrics), "slo_engine");
            work.finish(EvaluationOutcome::NoData);
        }
        {
            let mut work = EvaluationObservation::new(Some(&metrics), "raw-rule-name");
            work.finish(EvaluationOutcome::Error);
        }
        let text = metrics.render_prometheus();
        assert!(text.contains(
            "rush_engine_evaluation_last_success_timestamp_seconds{engine=\"slo_engine\"}"
        ));
        assert!(!text.contains("last_success_timestamp_seconds{engine=\"other\"}"));
        assert!(!text.contains("raw-rule-name"));
    }
}
