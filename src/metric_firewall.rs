//! Ingest-time metric firewall.
//!
//! Rules are evaluated against every metric datapoint as it flows through
//! `ChWriter` (covering OTLP, Datadog, and Prometheus ingest uniformly). Each
//! rule either:
//!   - **allows** the datapoint — a matching series is exempt from all block
//!     rules (enables allowlist mode: one catch-all block rule + allow rules),
//!   - **blocks** the datapoint (drops the whole series), matched by metric name
//!     (literal/regex) and/or a label key+value (literal/regex), or
//!   - **drops labels** from the series whose key matches a literal/regex,
//!     optionally scoped to a metric/label match.
//!
//! Precedence is fixed: allow → block. Drop-label rules always apply, even to
//! allowed series — allow exempts from blocking, not from label stripping.
//!
//! Compiled rules live behind `Arc<RwLock<Arc<MetricFirewall>>>` in `ChWriter`
//! and are hot-swapped by a background refresher / on config change, so the
//! ingest hot path only takes a brief read lock + cheap Arc clone.

use regex::Regex;

/// A metric row the firewall can inspect/mutate. Implemented by the metric
/// insert row structs in `models::ingest`.
pub trait MetricRow {
    fn fw_metric_name(&self) -> &str;
    fn fw_attributes(&self) -> &[(String, String)];
    fn fw_attributes_mut(&mut self) -> &mut Vec<(String, String)>;
}

/// A name/value matcher: match anything, an exact string, or a regex.
#[derive(Clone)]
enum Matcher {
    Any,
    Literal(String),
    Regex(Regex),
}

impl Matcher {
    fn build(pattern: &str, is_regex: bool) -> Option<Self> {
        if pattern.is_empty() {
            return Some(Matcher::Any);
        }
        if is_regex {
            match Regex::new(pattern) {
                Ok(re) => Some(Matcher::Regex(re)),
                Err(e) => {
                    tracing::warn!(pattern = %pattern, error = %e, "metric firewall: invalid regex, rule disabled");
                    None
                }
            }
        } else {
            Some(Matcher::Literal(pattern.to_string()))
        }
    }
    fn matches(&self, s: &str) -> bool {
        match self {
            Matcher::Any => true,
            Matcher::Literal(l) => l == s,
            Matcher::Regex(re) => re.is_match(s),
        }
    }
}

#[derive(Clone)]
enum CompiledAction {
    /// Exempt matching series from all block rules.
    Allow,
    /// Drop matching series entirely.
    Block,
    /// Strip label KEYS matching the inner matcher from matching series.
    DropLabel(Matcher),
}

#[derive(Clone)]
struct CompiledRule {
    action: CompiledAction,
    /// Metric-name match (Any = any metric).
    metric: Matcher,
    /// Optional label condition: series must carry `label_key` whose value
    /// matches `label_value` (Any value if the pattern was empty). None = no
    /// label condition.
    label_key: Option<String>,
    label_value: Matcher,
}

impl CompiledRule {
    /// Does this rule's match predicate apply to the given series?
    fn predicate_matches(&self, name: &str, attrs: &[(String, String)]) -> bool {
        if !self.metric.matches(name) {
            return false;
        }
        if let Some(key) = &self.label_key {
            match attrs.iter().find(|(k, _)| k == key) {
                Some((_, v)) => self.label_value.matches(v),
                None => false,
            }
        } else {
            true
        }
    }
}

/// A set of rules for one action, with a literal-metric-name fast path: rules
/// whose metric matcher is an exact string are bucketed by name so a row only
/// pays a HashMap probe for them; regex/any-metric rules stay in a scan list.
#[derive(Clone, Default)]
struct RuleSet {
    by_literal: std::collections::HashMap<String, Vec<CompiledRule>>,
    scan: Vec<CompiledRule>,
}

impl RuleSet {
    fn is_empty(&self) -> bool {
        self.by_literal.is_empty() && self.scan.is_empty()
    }

    fn push(&mut self, rule: CompiledRule) {
        if let Matcher::Literal(name) = &rule.metric {
            self.by_literal.entry(name.clone()).or_default().push(rule);
        } else {
            self.scan.push(rule);
        }
    }

    fn matches(&self, name: &str, attrs: &[(String, String)]) -> bool {
        if let Some(rules) = self.by_literal.get(name) {
            if rules.iter().any(|r| r.predicate_matches(name, attrs)) {
                return true;
            }
        }
        self.scan.iter().any(|r| r.predicate_matches(name, attrs))
    }
}

/// A compiled, ready-to-evaluate set of firewall rules, pre-partitioned by
/// action so `apply` never scans rules of the wrong kind.
#[derive(Clone, Default)]
pub struct MetricFirewall {
    allow: RuleSet,
    block: RuleSet,
    drop: Vec<CompiledRule>,
}

/// Raw rule fields as stored in `config_metric_firewall` (also the API shape).
pub struct RawRule {
    pub enabled: bool,
    pub action: String, // "allow" | "block" | "drop_label"
    pub metric_pattern: String,
    pub metric_regex: bool,
    pub match_label_key: String,
    pub match_label_value: String,
    pub match_label_value_regex: bool,
    pub drop_label_pattern: String,
    pub drop_label_regex: bool,
}

impl MetricFirewall {
    /// Compile raw rules; invalid-regex rules are skipped (logged) rather than
    /// failing the whole set.
    pub fn compile(raw: &[RawRule]) -> Self {
        let mut fw = MetricFirewall::default();
        for r in raw {
            if !r.enabled {
                continue;
            }
            let Some(metric) = Matcher::build(&r.metric_pattern, r.metric_regex) else { continue };
            let label_key = if r.match_label_key.is_empty() { None } else { Some(r.match_label_key.clone()) };
            let Some(label_value) = Matcher::build(&r.match_label_value, r.match_label_value_regex) else { continue };

            let action = match r.action.as_str() {
                "allow" => {
                    // An allow rule with no criteria would exempt everything and
                    // silently neuter every block rule — refuse it (the API also
                    // rejects this; guard here for rows written by other paths).
                    if matches!(metric, Matcher::Any) && label_key.is_none() {
                        tracing::warn!("metric firewall: allow rule has no match criteria, skipping");
                        continue;
                    }
                    CompiledAction::Allow
                }
                "drop_label" => {
                    // A drop-label rule needs a non-empty drop pattern, otherwise it
                    // would strip every label — refuse that (require an explicit key).
                    if r.drop_label_pattern.is_empty() {
                        tracing::warn!("metric firewall: drop_label rule has empty label pattern, skipping");
                        continue;
                    }
                    match Matcher::build(&r.drop_label_pattern, r.drop_label_regex) {
                        Some(m) => CompiledAction::DropLabel(m),
                        None => continue,
                    }
                }
                // Unknown action strings have always compiled as block; keep that.
                _ => CompiledAction::Block,
            };

            let rule = CompiledRule { action, metric, label_key, label_value };
            match &rule.action {
                CompiledAction::Allow => fw.allow.push(rule),
                CompiledAction::Block => fw.block.push(rule),
                CompiledAction::DropLabel(_) => fw.drop.push(rule),
            }
        }
        fw
    }

    pub fn is_empty(&self) -> bool {
        self.allow.is_empty() && self.block.is_empty() && self.drop.is_empty()
    }

    /// Apply all rules to a batch of metric rows in place. Returns the number of
    /// datapoints dropped (for logging/metrics). Rules are pre-partitioned by
    /// action, and literal-name rules are probed via HashMap, so each row scans
    /// only regex/any-metric rules of the relevant action.
    pub fn apply<T: MetricRow>(&self, rows: &mut Vec<T>) -> usize {
        if self.is_empty() {
            return 0;
        }
        let before = rows.len();
        rows.retain_mut(|row| {
            // Allow rules take precedence over block rules: a series matching any
            // allow rule cannot be blocked (labels may still be stripped below).
            if !self.block.is_empty() {
                let allowed = !self.allow.is_empty()
                    && self.allow.matches(row.fw_metric_name(), row.fw_attributes());
                if !allowed && self.block.matches(row.fw_metric_name(), row.fw_attributes()) {
                    return false; // drop the whole datapoint
                }
            }
            for rule in &self.drop {
                if rule.predicate_matches(row.fw_metric_name(), row.fw_attributes()) {
                    if let CompiledAction::DropLabel(dl) = &rule.action {
                        row.fw_attributes_mut().retain(|(k, _)| !dl.matches(k));
                    }
                }
            }
            true
        });
        before - rows.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Row { name: String, attrs: Vec<(String, String)> }
    impl MetricRow for Row {
        fn fw_metric_name(&self) -> &str { &self.name }
        fn fw_attributes(&self) -> &[(String, String)] { &self.attrs }
        fn fw_attributes_mut(&mut self) -> &mut Vec<(String, String)> { &mut self.attrs }
    }
    fn row(name: &str, attrs: &[(&str, &str)]) -> Row {
        Row { name: name.into(), attrs: attrs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect() }
    }
    fn raw(action: &str, mp: &str, mre: bool, lk: &str, lv: &str, lvre: bool, dl: &str, dlre: bool) -> RawRule {
        RawRule {
            enabled: true, action: action.into(),
            metric_pattern: mp.into(), metric_regex: mre,
            match_label_key: lk.into(), match_label_value: lv.into(), match_label_value_regex: lvre,
            drop_label_pattern: dl.into(), drop_label_regex: dlre,
        }
    }

    #[test]
    fn block_by_metric_literal_and_regex() {
        let fw = MetricFirewall::compile(&[
            raw("block", "go_gc_duration_seconds", false, "", "", false, "", false),
            raw("block", "^node_.*", true, "", "", false, "", false),
        ]);
        let mut rows = vec![
            row("go_gc_duration_seconds", &[]),
            row("node_cpu_seconds", &[]),
            row("http_requests_total", &[]),
        ];
        let dropped = fw.apply(&mut rows);
        assert_eq!(dropped, 2);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "http_requests_total");
    }

    #[test]
    fn block_by_label_value_regex() {
        // Block any series with env label matching dev/staging
        let fw = MetricFirewall::compile(&[raw("block", "", false, "env", "^(dev|staging)$", true, "", false)]);
        let mut rows = vec![
            row("m", &[("env", "prod")]),
            row("m", &[("env", "dev")]),
            row("m", &[("region", "us")]), // no env label → kept
        ];
        assert_eq!(fw.apply(&mut rows), 1);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.attrs.iter().all(|(_, v)| v != "dev")));
    }

    #[test]
    fn drop_labels_by_regex_scoped_to_metric() {
        // On http_* metrics, strip any label whose key starts with "tmp_"
        let fw = MetricFirewall::compile(&[raw("drop_label", "^http_.*", true, "", "", false, "^tmp_.*", true)]);
        let mut rows = vec![
            row("http_requests_total", &[("method", "GET"), ("tmp_debug", "1"), ("tmp_id", "x")]),
            row("cpu", &[("tmp_debug", "1")]), // not http_* → untouched
        ];
        let dropped = fw.apply(&mut rows);
        assert_eq!(dropped, 0); // drop_label never removes datapoints
        assert_eq!(rows[0].attrs, vec![("method".to_string(), "GET".to_string())]);
        assert_eq!(rows[1].attrs.len(), 1); // untouched
    }

    #[test]
    fn empty_firewall_is_noop() {
        let fw = MetricFirewall::compile(&[]);
        let mut rows = vec![row("m", &[("a", "b")])];
        assert_eq!(fw.apply(&mut rows), 0);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn allowlist_mode_catch_all_block_plus_allow() {
        // "Block all, then allow": a match-everything block rule with allow
        // exemptions. Rule order must not matter (allow has fixed precedence).
        let fw = MetricFirewall::compile(&[
            raw("block", "", false, "", "", false, "", false), // catch-all block
            raw("allow", "^http_.*", true, "", "", false, "", false),
            raw("allow", "", false, "team", "core", false, "", false),
        ]);
        let mut rows = vec![
            row("http_requests_total", &[]),          // allowed by metric
            row("node_cpu_seconds", &[("team", "core")]), // allowed by label
            row("node_cpu_seconds", &[("team", "web")]),  // blocked
            row("go_goroutines", &[]),                    // blocked
        ];
        assert_eq!(fw.apply(&mut rows), 2);
        let names: Vec<_> = rows.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["http_requests_total", "node_cpu_seconds"]);
        assert_eq!(rows[1].attrs[0].1, "core");
    }

    #[test]
    fn allow_exempts_blocking_but_not_label_stripping() {
        let fw = MetricFirewall::compile(&[
            raw("allow", "^http_.*", true, "", "", false, "", false),
            raw("block", "^http_.*", true, "", "", false, "", false),
            raw("drop_label", "", false, "", "", false, "^tmp_.*", true),
        ]);
        let mut rows = vec![row("http_requests_total", &[("method", "GET"), ("tmp_debug", "1")])];
        assert_eq!(fw.apply(&mut rows), 0); // allow beats block
        assert_eq!(rows.len(), 1);
        // ...but drop_label still applied to the allowed series.
        assert_eq!(rows[0].attrs, vec![("method".to_string(), "GET".to_string())]);
    }

    #[test]
    fn allow_without_block_changes_nothing() {
        let fw = MetricFirewall::compile(&[raw("allow", "^http_.*", true, "", "", false, "", false)]);
        let mut rows = vec![row("http_requests_total", &[]), row("node_cpu", &[])];
        assert_eq!(fw.apply(&mut rows), 0);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn match_all_allow_rule_is_refused_at_compile() {
        // An allow rule with no criteria would neuter every block rule — the
        // compiler must skip it so blocks still apply.
        let fw = MetricFirewall::compile(&[
            raw("allow", "", false, "", "", false, "", false), // skipped
            raw("block", "^node_.*", true, "", "", false, "", false),
        ]);
        let mut rows = vec![row("node_cpu", &[]), row("http_x", &[])];
        assert_eq!(fw.apply(&mut rows), 1);
        assert_eq!(rows[0].name, "http_x");
    }
}
