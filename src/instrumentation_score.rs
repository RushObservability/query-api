//! The open Instrumentation Score (https://github.com/instrumentation-score/spec).
//! Only spec rules count toward this score, with the spec's weights. Rules Rush
//! cannot evaluate from its bounded sample are listed as not implemented, as the
//! spec requires, and never count as passed.
use serde::{Deserialize, Serialize};

use crate::handlers::telemetry_quality::RuleEvaluation;

/// The spec revision these rules and criteria were taken from.
pub const SPEC: &str = "instrumentation-score/spec@ef080a8 (2026-10-30)";

/// MET-007: resource keys that must not appear on metric data.
pub const MET_007_KEYS: [&str; 6] = [
    "host.ip",
    "host.mac",
    "process.command_args",
    "process.command_line",
    "process.runtime.description",
    "os.description",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StandardRule {
    pub id: String,
    pub description: String,
    pub impact: String,
    pub weight: u32,
    pub implemented: bool,
    /// For rules that are not implemented: why Rush cannot evaluate them yet.
    pub note: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstrumentationScore {
    pub value: Option<f64>,
    pub category: Option<String>,
    pub passed_weight: u32,
    pub evaluated_weight: u32,
    pub evaluated_rules: usize,
    pub not_evaluated_rules: usize,
}

pub fn rules() -> Vec<StandardRule> {
    let rule = |id: &str, description: &str, impact: &str, note: &str| StandardRule {
        id: id.into(),
        description: description.into(),
        impact: impact.into(),
        weight: weight(impact),
        implemented: note.is_empty(),
        note: note.into(),
    };
    let pii = "Needs sensitive-data detection over values Rush's quality scan does not read.";
    let traces = "Needs complete traces; Rush samples individual spans.";
    let semconv = "Needs the semantic-conventions attribute registry.";
    vec![
        rule("RES-005", "service.name is present.", "Critical", ""),
        rule(
            "LOG-003",
            "Log records do not contain sensitive data.",
            "Critical",
            pii,
        ),
        rule(
            "MET-008",
            "Metrics do not contain sensitive data.",
            "Critical",
            pii,
        ),
        rule(
            "RES-010",
            "Resource attribute values do not contain sensitive data.",
            "Critical",
            pii,
        ),
        rule(
            "SPA-006",
            "Spans do not contain sensitive data.",
            "Critical",
            pii,
        ),
        rule(
            "RES-007",
            "deployment.environment.name is present.",
            "Important",
            "",
        ),
        rule(
            "RES-003",
            "k8s.pod.uid is present on telemetry from Kubernetes pods.",
            "Important",
            "",
        ),
        rule(
            "LOG-002",
            "Log records have their severity set.",
            "Important",
            "",
        ),
        rule(
            "MET-003",
            "Each metric name uses one unit.",
            "Important",
            "",
        ),
        rule(
            "SPA-004",
            "Root spans are not CLIENT spans.",
            "Important",
            "",
        ),
        rule(
            "LOG-001",
            "Debug logs are not enabled in production for longer than 14 days.",
            "Important",
            "Needs 14 days of log history per environment.",
        ),
        rule(
            "MET-001",
            "Metric attributes have fewer than 10,000 values per hour.",
            "Important",
            "Needs exact distinct-value counts over a full hour.",
        ),
        rule(
            "MET-002",
            "Metrics have a non-default UCUM unit.",
            "Important",
            "UCUM unit validation is not implemented.",
        ),
        rule(
            "MET-006",
            "Metric names do not equal semantic-convention attribute keys.",
            "Important",
            semconv,
        ),
        rule(
            "RES-002",
            "service.instance.id is unique across logical resources.",
            "Important",
            "Needs instance-to-resource mapping across the window.",
        ),
        rule(
            "RES-004",
            "Semantic-convention attributes are used at the right level.",
            "Important",
            semconv,
        ),
        rule(
            "SPA-003",
            "Span names have bounded cardinality.",
            "Important",
            "The specification has not defined this rule's criteria yet.",
        ),
        rule(
            "SPA-005",
            "Traces have at most 20 spans shorter than 5 ms.",
            "Important",
            traces,
        ),
        rule("RES-001", "service.instance.id is present.", "Normal", ""),
        rule(
            "RES-006",
            "service.criticality uses its exact lowercase enum value.",
            "Normal",
            "",
        ),
        rule(
            "MET-004",
            "Histograms use the same buckets per metric name.",
            "Normal",
            "",
        ),
        rule(
            "MET-005",
            "Metric names do not contain the unit.",
            "Normal",
            "",
        ),
        rule(
            "MET-007",
            "Metric resources omit host.ip, host.mac, process command lines and OS descriptions.",
            "Normal",
            "",
        ),
        rule(
            "SPA-001",
            "Traces have at most 10 INTERNAL spans per service.",
            "Normal",
            traces,
        ),
        rule(
            "SPA-002",
            "Traces do not contain orphan spans.",
            "Normal",
            traces,
        ),
        rule(
            "SDK-001",
            "Language and runtime versions are supported by the SDK.",
            "Low",
            "Needs the SDK support matrix.",
        ),
    ]
}

fn weight(impact: &str) -> u32 {
    match impact {
        "Critical" => 40,
        "Important" => 30,
        "Normal" => 20,
        _ => 10,
    }
}

/// Spec formula: Σ(passed × weight) / Σ(evaluated × weight) × 100, to two decimals.
pub fn score(checks: &[RuleEvaluation], rules: &[StandardRule]) -> InstrumentationScore {
    let mut result = InstrumentationScore::default();
    for check in checks {
        if check.status == "not_evaluated" {
            result.not_evaluated_rules += 1;
            continue;
        }
        let weight = rules
            .iter()
            .find(|r| r.id == check.rule_id)
            .map_or(0, |r| r.weight);
        result.evaluated_rules += 1;
        result.evaluated_weight += weight;
        if check.status == "pass" {
            result.passed_weight += weight;
        }
    }
    if result.evaluated_weight > 0 {
        let value = f64::from(result.passed_weight) / f64::from(result.evaluated_weight) * 100.0;
        result.value = Some((value * 100.0).round() / 100.0);
        result.category = result.value.map(category);
    }
    result
}

/// Organization view: the mean of the service scores that could be computed.
pub fn mean(services: &[InstrumentationScore]) -> InstrumentationScore {
    let values: Vec<f64> = services.iter().filter_map(|s| s.value).collect();
    let mut result = InstrumentationScore {
        evaluated_rules: services.iter().map(|s| s.evaluated_rules).sum(),
        not_evaluated_rules: services.iter().map(|s| s.not_evaluated_rules).sum(),
        passed_weight: services.iter().map(|s| s.passed_weight).sum(),
        evaluated_weight: services.iter().map(|s| s.evaluated_weight).sum(),
        ..Default::default()
    };
    if !values.is_empty() {
        let value = values.iter().sum::<f64>() / values.len() as f64;
        result.value = Some((value * 100.0).round() / 100.0);
        result.category = result.value.map(category);
    }
    result
}

pub fn category(score: f64) -> String {
    match score {
        s if s >= 90.0 => "Excellent",
        s if s >= 75.0 => "Good",
        s if s >= 50.0 => "Needs Improvement",
        _ => "Poor",
    }
    .into()
}

/// RES-006: a value matching a well-known level once trimmed and lowercased
/// must already be that exact lowercase value. Absent and custom values pass.
pub fn criticality_ok(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    !["critical", "high", "medium", "low"].contains(&normalized.as_str()) || value == normalized
}

/// MET-005: a dot- or underscore-separated part of the metric name is a unit,
/// either the metric's own unit or a common spelled-out or abbreviated unit.
pub fn unit_in_name(name: &str, unit: &str) -> bool {
    const UNITS: [&str; 28] = [
        "seconds",
        "second",
        "secs",
        "milliseconds",
        "millisecond",
        "millis",
        "ms",
        "microseconds",
        "us",
        "nanoseconds",
        "ns",
        "minutes",
        "hours",
        "bytes",
        "byte",
        "kilobytes",
        "megabytes",
        "gigabytes",
        "kb",
        "mb",
        "gb",
        "kib",
        "mib",
        "gib",
        "bits",
        "percent",
        "celsius",
        "hertz",
    ];
    let unit = unit.trim().to_ascii_lowercase();
    name.to_ascii_lowercase().split(['.', '_']).any(|part| {
        UNITS.contains(&part)
            || (!unit.is_empty() && unit != "1" && !unit.starts_with('{') && part == unit)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(id: &str, status: &str) -> RuleEvaluation {
        RuleEvaluation {
            rule_id: id.into(),
            status: status.into(),
            sampled: 1,
            affected: u64::from(status == "finding"),
        }
    }

    #[test]
    fn score_uses_spec_weights_and_skips_unevaluated_rules() {
        let rules = rules();
        let checks = [
            eval("RES-005", "pass"),
            eval("RES-007", "finding"),
            eval("RES-001", "pass"),
            eval("MET-004", "not_evaluated"),
        ];
        let result = score(&checks, &rules);
        // (40 + 20) / (40 + 30 + 20) × 100
        assert_eq!(result.value, Some(66.67));
        assert_eq!(result.category.as_deref(), Some("Needs Improvement"));
        assert_eq!(result.not_evaluated_rules, 1);
        assert_eq!(score(&checks[3..], &rules).value, None);
    }

    #[test]
    fn rule_table_matches_the_spec() {
        let rules = rules();
        assert_eq!(rules.len(), 26);
        assert_eq!(rules.iter().filter(|r| r.implemented).count(), 11);
        let critical = rules.iter().filter(|r| r.impact == "Critical").count();
        assert_eq!(critical, 5);
        assert!(rules.iter().all(|r| r.implemented == r.note.is_empty()));
    }

    #[test]
    fn criticality_and_unit_names_follow_the_criteria() {
        assert!(criticality_ok("") && criticality_ok("high") && criticality_ok("tier-1"));
        assert!(!criticality_ok("High") && !criticality_ok(" low"));
        assert!(unit_in_name("collection.duration.seconds", "s"));
        assert!(unit_in_name("http_server_duration_ms", "ms"));
        assert!(unit_in_name("queue.depth.items", "items"));
        assert!(!unit_in_name("http.server.request.duration", "s"));
        assert!(!unit_in_name("process.cpu.count", "{cpu}"));
    }
}
