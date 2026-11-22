use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};

use super::{FirewallRuleInput, forbid_block_everything, policy_revision, validate};
use crate::{
    AppState,
    clickhouse_config::MetricFirewallRule,
    handlers::users::require_admin,
    metric_firewall::{MetricFirewall, MetricRow, RawRule},
    promql::build_label_set,
};

pub const MAX_PREVIEW_BODY_BYTES: usize = 128 * 1024;
const MAX_SAMPLES: usize = 100;
const MAX_LABELS: usize = 64;
type Labels = BTreeMap<String, String>;
type PreviewError = (StatusCode, String);

#[derive(Deserialize)]
pub struct PreviewInput {
    /// If set, replace this rule in its existing position in the draft policy.
    rule_id: Option<String>,
    rule: FirewallRuleInput,
    samples: Vec<Sample>,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
struct Sample {
    metric_name: String,
    #[serde(default)]
    service_name: String,
    #[serde(default)]
    labels: Labels,
}

struct SampleRow<'a> {
    metric_name: &'a str,
    attributes: Vec<(String, String)>,
}

impl MetricRow for SampleRow<'_> {
    fn fw_metric_name(&self) -> &str {
        self.metric_name
    }

    fn fw_attributes(&self) -> &[(String, String)] {
        &self.attributes
    }

    fn fw_attributes_mut(&mut self) -> &mut Vec<(String, String)> {
        &mut self.attributes
    }
}

#[derive(Serialize)]
struct Outcome {
    /// Null represents a blocked series.
    labels: Option<Labels>,
    matched_rules: Vec<String>,
    #[serde(skip)]
    identity: Option<Labels>,
}

#[derive(Serialize)]
struct PreviewRow {
    input: Sample,
    current: Outcome,
    proposed: Outcome,
    collision_id: Option<usize>,
}

#[derive(Serialize)]
struct Collision {
    id: usize,
    identity: Labels,
    series_count: usize,
    introduced: bool,
}

#[derive(Serialize)]
pub struct PreviewResult {
    policy_revision: String,
    sampled: usize,
    duplicates: usize,
    changed: usize,
    blocked: usize,
    output_identities: usize,
    rows: Vec<PreviewRow>,
    collisions: Vec<Collision>,
}

/// POST /api/v1/metric-firewall/preview
///
/// Like CRUD, this is deployment-wide and requires an admin session. The
/// endpoint evaluates only user-supplied samples. It never queries tenant
/// telemetry, saves the draft, or updates the ingest writer or its counters.
pub async fn preview(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<PreviewInput>,
) -> Result<Json<PreviewResult>, PreviewError> {
    require_admin(&state, &headers).await?;
    validate_preview(&input)?;
    let current = state
        .config_db
        .list_metric_firewall()
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?;
    let proposed = draft_policy(&current, input.rule_id.as_deref(), &input.rule)?;
    Ok(Json(evaluate(input.samples, &current, &proposed)))
}

fn validate_preview(input: &PreviewInput) -> Result<(), PreviewError> {
    if input.samples.is_empty() || input.samples.len() > MAX_SAMPLES {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Provide between 1 and {MAX_SAMPLES} sample series."),
        ));
    }
    for (index, sample) in input.samples.iter().enumerate() {
        if sample.metric_name.trim().is_empty()
            || sample.metric_name.len() > 512
            || sample.service_name.len() > 512
            || sample.labels.len() > MAX_LABELS
            || sample
                .labels
                .iter()
                .any(|(key, value)| key.trim().is_empty() || key.len() > 256 || value.len() > 2048)
        {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Sample {} has an invalid metric name or labels.", index + 1),
            ));
        }
    }
    let rule = &input.rule;
    if [
        &rule.name,
        &rule.metric_pattern,
        &rule.match_label_key,
        &rule.match_label_value,
        &rule.drop_label_pattern,
    ]
    .iter()
    .any(|value| value.len() > 2048)
    {
        return Err((StatusCode::BAD_REQUEST, "Draft rule is too long.".into()));
    }
    Ok(())
}

fn draft_policy(
    current: &[MetricFirewallRule],
    rule_id: Option<&str>,
    rule: &FirewallRuleInput,
) -> Result<Vec<MetricFirewallRule>, PreviewError> {
    let mut proposed = current.to_vec();
    if let Some(id) = rule_id {
        let entry = proposed
            .iter_mut()
            .find(|entry| entry.id == id)
            .ok_or_else(|| (StatusCode::NOT_FOUND, "Firewall rule not found.".into()))?;
        *entry = validate(rule, id.to_owned(), entry.created_at.clone())?;
    } else {
        proposed.push(validate(rule, String::new(), String::new())?);
    }
    forbid_block_everything(&proposed)?;
    Ok(proposed)
}

fn compile(rules: &[MetricFirewallRule]) -> MetricFirewall {
    let raw: Vec<_> = rules.iter().map(RawRule::from).collect();
    MetricFirewall::compile(&raw)
}

fn outcome(sample: &Sample, firewall: &MetricFirewall, rules: &[MetricFirewallRule]) -> Outcome {
    let mut row = SampleRow {
        metric_name: &sample.metric_name,
        attributes: sample.labels.clone().into_iter().collect(),
    };
    let mut matched_rules = Vec::new();
    let kept = firewall.apply_row(&mut row, |index| {
        matched_rules.push(rules[index].name.clone());
    });
    let identity =
        kept.then(|| build_label_set(&sample.metric_name, &sample.service_name, &row.attributes));
    Outcome {
        labels: kept.then(|| row.attributes.into_iter().collect()),
        matched_rules,
        identity,
    }
}

fn evaluate(
    samples: Vec<Sample>,
    current: &[MetricFirewallRule],
    proposed: &[MetricFirewallRule],
) -> PreviewResult {
    let current_firewall = compile(current);
    let proposed_firewall = compile(proposed);
    let mut seen = BTreeSet::new();
    let supplied = samples.len();
    let mut rows: Vec<_> = samples
        .into_iter()
        .filter(|sample| seen.insert(sample.clone()))
        .map(|input| PreviewRow {
            current: outcome(&input, &current_firewall, current),
            proposed: outcome(&input, &proposed_firewall, proposed),
            input,
            collision_id: None,
        })
        .collect();

    let mut groups: BTreeMap<Labels, Vec<usize>> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        if let Some(identity) = &row.proposed.identity {
            groups.entry(identity.clone()).or_default().push(index);
        }
    }
    let output_identities = groups.len();
    let mut collisions = Vec::new();
    for (identity, members) in groups.into_iter().filter(|(_, members)| members.len() > 1) {
        let first = &rows[members[0]].current.identity;
        let introduced = first.is_none()
            || members
                .iter()
                .any(|index| &rows[*index].current.identity != first);
        let id = collisions.len() + 1;
        for &index in &members {
            rows[index].collision_id = Some(id);
        }
        collisions.push(Collision {
            id,
            identity,
            series_count: members.len(),
            introduced,
        });
    }
    PreviewResult {
        policy_revision: policy_revision(current),
        sampled: rows.len(),
        duplicates: supplied - rows.len(),
        changed: rows
            .iter()
            .filter(|row| row.current.labels != row.proposed.labels)
            .count(),
        blocked: rows
            .iter()
            .filter(|row| row.proposed.labels.is_none())
            .count(),
        output_identities,
        rows,
        collisions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn parse_rule(input: Value) -> FirewallRuleInput {
        serde_json::from_value(input).unwrap()
    }

    fn samples() -> Vec<Sample> {
        serde_json::from_value(json!([
            {"metric_name": "http_requests_total", "service_name": "payments", "labels": {"pod_uid": "p-15"}},
            {"metric_name": "http_requests_total", "service_name": "payments", "labels": {"pod_uid": "p-47"}},
            {"metric_name": "http_requests_total", "service_name": "orders", "labels": {"pod_uid": "p-15"}},
            {"metric_name": "http_requests_total", "service_name": "orders", "labels": {"pod_uid": "p-15"}}
        ])).unwrap()
    }

    #[test]
    fn preview_reports_only_distinct_inputs_collapsing_into_the_same_output() {
        let strip_pod = parse_rule(json!({
            "name": "strip pod",
            "action": "drop_label",
            "drop_label_pattern": "pod_uid",
        }));
        let proposed = draft_policy(&[], None, &strip_pod).unwrap();
        let result = evaluate(samples(), &[], &proposed);
        assert_eq!(result.sampled, 3);
        assert_eq!(result.duplicates, 1);
        assert_eq!(result.changed, 3);
        assert_eq!(result.blocked, 0);
        assert_eq!(result.output_identities, 2);
        assert_eq!(result.collisions.len(), 1);
        assert_eq!(result.collisions[0].identity["service_name"], "payments");
        assert!(result.collisions[0].introduced);
        assert_eq!(result.rows[0].collision_id, Some(1));
        assert_eq!(result.rows[1].collision_id, Some(1));
        assert_eq!(result.rows[2].collision_id, None);
        assert_eq!(result.rows[0].proposed.matched_rules, ["strip pod"]);

        let existing_collision = evaluate(samples(), &proposed, &proposed);
        assert_eq!(existing_collision.changed, 0);
        assert!(!existing_collision.collisions[0].introduced);
    }

    #[test]
    fn replacing_an_allow_rule_reuses_ingest_precedence_and_validates_the_policy() {
        let allow = parse_rule(json!({
            "name": "allow requests",
            "action": "allow",
            "metric_pattern": "http_requests_total",
        }));
        let block = parse_rule(json!({ "name": "block all", "action": "block" }));
        let strip = parse_rule(json!({
            "name": "strip p labels",
            "action": "drop_label",
            "drop_label_pattern": "^pod_",
            "drop_label_regex": true,
        }));
        let current = vec![
            validate(&allow, "allow".into(), "1".into()).unwrap(),
            validate(&block, "block".into(), "2".into()).unwrap(),
            validate(&strip, "strip".into(), "3".into()).unwrap(),
        ];
        let narrowed_allow = parse_rule(json!({
            "name": "allow one pod",
            "action": "allow",
            "match_label_key": "pod_uid",
            "match_label_value": "p-15",
        }));
        let proposed = draft_policy(&current, Some("allow"), &narrowed_allow).unwrap();
        assert_eq!(proposed.len(), current.len());
        let result = evaluate(samples(), &current, &proposed);
        assert_eq!(result.changed, 1);
        assert_eq!(result.blocked, 1);
        assert!(result.collisions.is_empty());
        assert_eq!(
            result.rows[0].current.matched_rules,
            ["allow requests", "strip p labels"]
        );
        assert_eq!(
            result.rows[0].proposed.matched_rules,
            ["allow one pod", "strip p labels"]
        );
        assert_eq!(result.rows[1].proposed.matched_rules, ["block all"]);
        assert!(result.rows[1].proposed.labels.is_none());

        let mut disabled = narrowed_allow;
        disabled.enabled = false;
        assert_eq!(
            draft_policy(&current, Some("allow"), &disabled)
                .unwrap_err()
                .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            draft_policy(&current, Some("missing"), &disabled)
                .unwrap_err()
                .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            super::super::check_policy_revision(Some(&result.policy_revision), &proposed)
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );
    }

    #[test]
    fn preview_rejects_oversized_samples_and_invalid_drafts() {
        let invalid = parse_rule(json!({
            "name": "broken matcher",
            "action": "block",
            "metric_pattern": "[",
            "metric_regex": true,
        }));
        assert_eq!(
            draft_policy(&[], None, &invalid).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
        let input = PreviewInput {
            rule_id: None,
            rule: invalid,
            samples: samples()
                .into_iter()
                .cycle()
                .take(MAX_SAMPLES + 1)
                .collect(),
        };
        assert_eq!(
            validate_preview(&input).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
    }
}
