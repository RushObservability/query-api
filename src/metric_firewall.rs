//! Ingest-time metric firewall.
//!
//! Rules are evaluated against every metric datapoint as it flows through
//! `ChWriter` (covering OTLP, Datadog, and Prometheus ingest uniformly). Each
//! rule either:
//!   - **blocks** the datapoint (drops the whole series), matched by metric name
//!     (literal/regex) and/or a label key+value (literal/regex), or
//!   - **drops labels** from the series whose key matches a literal/regex,
//!     optionally scoped to a metric/label match.
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
struct CompiledRule {
    block: bool,
    /// Metric-name match (Any = any metric).
    metric: Matcher,
    /// Optional label condition: series must carry `label_key` whose value
    /// matches `label_value` (Any value if the pattern was empty). None = no
    /// label condition.
    label_key: Option<String>,
    label_value: Matcher,
    /// For drop-label rules: which label KEYS to strip (Any = strip all labels
    /// on a matching series — guarded against below). None for block rules.
    drop_label: Option<Matcher>,
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

/// A compiled, ready-to-evaluate set of firewall rules.
#[derive(Clone, Default)]
pub struct MetricFirewall {
    rules: Vec<CompiledRule>,
}

/// Raw rule fields as stored in `config_metric_firewall` (also the API shape).
pub struct RawRule {
    pub enabled: bool,
    pub action: String, // "block" | "drop_label"
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
        let mut rules = Vec::new();
        for r in raw {
            if !r.enabled {
                continue;
            }
            let block = r.action != "drop_label";
            let Some(metric) = Matcher::build(&r.metric_pattern, r.metric_regex) else { continue };
            let label_key = if r.match_label_key.is_empty() { None } else { Some(r.match_label_key.clone()) };
            let Some(label_value) = Matcher::build(&r.match_label_value, r.match_label_value_regex) else { continue };

            let drop_label = if block {
                None
            } else {
                // A drop-label rule needs a non-empty drop pattern, otherwise it
                // would strip every label — refuse that (require an explicit key).
                if r.drop_label_pattern.is_empty() {
                    tracing::warn!("metric firewall: drop_label rule has empty label pattern, skipping");
                    continue;
                }
                match Matcher::build(&r.drop_label_pattern, r.drop_label_regex) {
                    Some(m) => Some(m),
                    None => continue,
                }
            };

            rules.push(CompiledRule { block, metric, label_key, label_value, drop_label });
        }
        MetricFirewall { rules }
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Apply all rules to a batch of metric rows in place. Returns the number of
    /// datapoints dropped (for logging/metrics).
    pub fn apply<T: MetricRow>(&self, rows: &mut Vec<T>) -> usize {
        if self.rules.is_empty() {
            return 0;
        }
        let before = rows.len();
        rows.retain_mut(|row| {
            for rule in &self.rules {
                if !rule.predicate_matches(row.fw_metric_name(), row.fw_attributes()) {
                    continue;
                }
                if rule.block {
                    return false; // drop the whole datapoint
                }
                if let Some(dl) = &rule.drop_label {
                    row.fw_attributes_mut().retain(|(k, _)| !dl.matches(k));
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
}
