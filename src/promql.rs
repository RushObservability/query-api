//! Minimal PromQL parser for Grafana compatibility.
//!
//! Supports:
//!   - Metric selectors: `metric_name{label="value", label2=~"regex"}`
//!   - Range functions: `rate(metric[5m])`, `irate(metric[5m])`, `increase(metric[5m])`
//!   - Aggregations: `sum by (label) (expr)`, `sum(expr) by (label)`
//!   - Nested: `sum by (service_name) (rate(http_requests_total[5m]))`

use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub enum PromExpr {
    Selector(MetricSelector),
    RangeFunction {
        func: RangeFunc,
        selector: MetricSelector,
        range_secs: f64,
    },
    Aggregation {
        op: AggOp,
        inner: Box<PromExpr>,
        by_labels: Vec<String>,
        without: bool,
    },
}

#[derive(Debug, Clone)]
pub struct MetricSelector {
    pub name: String,
    pub matchers: Vec<LabelMatcher>,
}

#[derive(Debug, Clone)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MatchOp {
    Eq,    // =
    Neq,   // !=
    Re,    // =~
    Nre,   // !~
}

#[derive(Debug, Clone, Copy)]
pub enum RangeFunc {
    Rate,
    Irate,
    Increase,
}

#[derive(Debug, Clone, Copy)]
pub enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

pub fn parse(input: &str) -> Result<PromExpr, String> {
    let input = input.trim();
    parse_expr(input)
}

fn parse_expr(input: &str) -> Result<PromExpr, String> {
    // Try aggregation first: sum/avg/min/max/count (...)
    if let Some(expr) = try_aggregation(input)? {
        return Ok(expr);
    }

    // Try range function: rate/irate/increase(selector[duration])
    if let Some(expr) = try_range_function(input)? {
        return Ok(expr);
    }

    // Plain selector: metric_name{labels}
    parse_selector(input).map(PromExpr::Selector)
}

fn try_aggregation(input: &str) -> Result<Option<PromExpr>, String> {
    let agg_ops = [
        ("sum", AggOp::Sum),
        ("avg", AggOp::Avg),
        ("min", AggOp::Min),
        ("max", AggOp::Max),
        ("count", AggOp::Count),
    ];

    for (keyword, op) in &agg_ops {
        if !input.starts_with(keyword) {
            continue;
        }
        let rest = input[keyword.len()..].trim_start();

        // Pattern 1: sum by (labels) (expr)
        // Pattern 2: sum(expr) by (labels)
        // Pattern 3: sum without (labels) (expr)
        // Pattern 4: sum(expr) without (labels)

        let (by_labels, without, inner_str) = if rest.starts_with("by") || rest.starts_with("without") {
            let is_without = rest.starts_with("without");
            let after_kw = if is_without { &rest[7..] } else { &rest[2..] };
            let after_kw = after_kw.trim_start();

            // Parse (label_list)
            let (labels, after_labels) = parse_paren_list(after_kw)?;
            let after_labels = after_labels.trim_start();

            // The rest should be (expr)
            let inner = strip_outer_parens(after_labels)?;
            (labels, is_without, inner)
        } else if rest.starts_with('(') {
            // sum(expr) possibly followed by by (labels) or without (labels)
            let inner_end = find_matching_paren(rest, 0)?;
            let inner = &rest[1..inner_end];
            let after = rest[inner_end + 1..].trim_start();

            if after.starts_with("by") || after.starts_with("without") {
                let is_without = after.starts_with("without");
                let after_kw = if is_without { &after[7..] } else { &after[2..] };
                let after_kw = after_kw.trim_start();
                let (labels, _) = parse_paren_list(after_kw)?;
                (labels, is_without, inner.to_string())
            } else {
                (vec![], false, inner.to_string())
            }
        } else {
            continue;
        };

        let inner_expr = parse_expr(&inner_str)?;
        return Ok(Some(PromExpr::Aggregation {
            op: *op,
            inner: Box::new(inner_expr),
            by_labels,
            without,
        }));
    }

    Ok(None)
}

fn try_range_function(input: &str) -> Result<Option<PromExpr>, String> {
    let funcs = [
        ("rate", RangeFunc::Rate),
        ("irate", RangeFunc::Irate),
        ("increase", RangeFunc::Increase),
    ];

    for (keyword, func) in &funcs {
        if !input.starts_with(keyword) {
            continue;
        }
        let rest = input[keyword.len()..].trim_start();
        if !rest.starts_with('(') {
            continue;
        }

        // Find matching closing paren
        let inner_end = find_matching_paren(rest, 0)?;
        let inner = &rest[1..inner_end];

        // Inner should be: selector[duration]
        let bracket_start = inner
            .rfind('[')
            .ok_or_else(|| format!("expected [duration] in {keyword}()"))?;
        let bracket_end = inner
            .rfind(']')
            .ok_or_else(|| format!("expected closing ] in {keyword}()"))?;

        let selector_str = &inner[..bracket_start].trim();
        let duration_str = &inner[bracket_start + 1..bracket_end];

        let selector = parse_selector(selector_str)?;
        let range_secs = parse_duration(duration_str)?;

        return Ok(Some(PromExpr::RangeFunction {
            func: *func,
            selector,
            range_secs,
        }));
    }

    Ok(None)
}

fn parse_selector(input: &str) -> Result<MetricSelector, String> {
    let input = input.trim();

    let (name, matchers_str) = if let Some(brace_start) = input.find('{') {
        let brace_end = input
            .rfind('}')
            .ok_or("unclosed { in selector")?;
        let name = input[..brace_start].trim().to_string();
        let matchers_str = &input[brace_start + 1..brace_end];
        (name, matchers_str)
    } else {
        (input.to_string(), "")
    };

    let matchers = if matchers_str.is_empty() {
        vec![]
    } else {
        parse_matchers(matchers_str)?
    };

    Ok(MetricSelector { name, matchers })
}

fn parse_matchers(input: &str) -> Result<Vec<LabelMatcher>, String> {
    let mut matchers = Vec::new();
    let mut chars = input.chars().peekable();

    loop {
        // Skip whitespace and commas
        while chars.peek().map_or(false, |c| *c == ' ' || *c == ',') {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }

        // Read label name
        let mut name = String::new();
        while chars.peek().map_or(false, |c| c.is_alphanumeric() || *c == '_' || *c == '.') {
            name.push(chars.next().unwrap());
        }
        if name.is_empty() {
            break;
        }

        // Skip whitespace
        while chars.peek() == Some(&' ') {
            chars.next();
        }

        // Read operator
        let op = match (chars.peek().copied(), {
            let mut clone = chars.clone();
            clone.next();
            clone.peek().copied()
        }) {
            (Some('='), Some('~')) => {
                chars.next();
                chars.next();
                MatchOp::Re
            }
            (Some('!'), Some('=')) => {
                chars.next();
                chars.next();
                MatchOp::Neq
            }
            (Some('!'), Some('~')) => {
                chars.next();
                chars.next();
                MatchOp::Nre
            }
            (Some('='), _) => {
                chars.next();
                MatchOp::Eq
            }
            _ => return Err(format!("expected operator after label '{name}'")),
        };

        // Skip whitespace
        while chars.peek() == Some(&' ') {
            chars.next();
        }

        // Read quoted value
        let quote = chars.next().ok_or("expected quoted value")?;
        if quote != '"' && quote != '\'' {
            return Err(format!("expected quoted value, got '{quote}'"));
        }

        let mut value = String::new();
        loop {
            match chars.next() {
                Some('\\') => {
                    if let Some(c) = chars.next() {
                        value.push(c);
                    }
                }
                Some(c) if c == quote => break,
                Some(c) => value.push(c),
                None => return Err("unterminated string in matcher".to_string()),
            }
        }

        matchers.push(LabelMatcher { name, op, value });
    }

    Ok(matchers)
}

fn parse_duration(input: &str) -> Result<f64, String> {
    let input = input.trim();
    let mut total_secs = 0.0;
    let mut num_str = String::new();

    for c in input.chars() {
        if c.is_ascii_digit() || c == '.' {
            num_str.push(c);
        } else {
            let num: f64 = num_str
                .parse()
                .map_err(|_| format!("invalid duration number: {num_str}"))?;
            num_str.clear();
            total_secs += match c {
                's' => num,
                'm' => num * 60.0,
                'h' => num * 3600.0,
                'd' => num * 86400.0,
                'w' => num * 604800.0,
                'y' => num * 31536000.0,
                _ => return Err(format!("unknown duration unit: {c}")),
            };
        }
    }

    if total_secs == 0.0 {
        return Err("empty duration".to_string());
    }
    Ok(total_secs)
}

fn parse_paren_list(input: &str) -> Result<(Vec<String>, String), String> {
    let input = input.trim();
    if !input.starts_with('(') {
        return Err("expected '(' for label list".to_string());
    }
    let close = input
        .find(')')
        .ok_or("unclosed '(' in label list")?;
    let inner = &input[1..close];
    let labels: Vec<String> = inner
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let rest = input[close + 1..].to_string();
    Ok((labels, rest))
}

fn strip_outer_parens(input: &str) -> Result<String, String> {
    let input = input.trim();
    if !input.starts_with('(') {
        return Err("expected '(' wrapping expression".to_string());
    }
    let end = find_matching_paren(input, 0)?;
    Ok(input[1..end].to_string())
}

fn find_matching_paren(input: &str, open_pos: usize) -> Result<usize, String> {
    let bytes = input.as_bytes();
    let mut depth = 0;
    for i in open_pos..bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
    }
    Err("unmatched parenthesis".to_string())
}

// ── Evaluation ──

/// A single time-series identified by its label set, with ordered samples.
#[derive(Debug, Clone)]
pub struct TimeSeries {
    pub labels: BTreeMap<String, String>,
    pub samples: Vec<(f64, f64)>, // (timestamp_secs, value)
}

/// Build the label set for a sample row, merging __name__, service_name, and attributes.
pub fn build_label_set(
    metric_name: &str,
    service_name: &str,
    attributes: &[(String, String)],
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("__name__".to_string(), metric_name.to_string());
    if !service_name.is_empty() {
        labels.insert("service_name".to_string(), service_name.to_string());
    }
    for (k, v) in attributes {
        if !v.is_empty() {
            labels.insert(k.clone(), v.clone());
        }
    }
    labels
}

/// Group raw samples into individual time series by their label set.
pub fn group_into_series(
    samples: Vec<(BTreeMap<String, String>, f64, f64)>,
) -> Vec<TimeSeries> {
    let mut map: BTreeMap<BTreeMap<String, String>, Vec<(f64, f64)>> = BTreeMap::new();
    for (labels, ts, val) in samples {
        map.entry(labels).or_default().push((ts, val));
    }
    map.into_iter()
        .map(|(labels, mut samples)| {
            samples.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            TimeSeries { labels, samples }
        })
        .collect()
}

/// Compute rate: (last - first) / (last_ts - first_ts) per second.
/// Handles counter resets by adding the decrease.
pub fn compute_rate(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let first = samples.first().unwrap();
    let last = samples.last().unwrap();
    let dt = last.0 - first.0;
    if dt <= 0.0 {
        return None;
    }

    // Track total increase, handling counter resets
    let mut total_increase = 0.0;
    for i in 1..samples.len() {
        let delta = samples[i].1 - samples[i - 1].1;
        if delta >= 0.0 {
            total_increase += delta;
        } else {
            // Counter reset: the new value is the increase from zero
            total_increase += samples[i].1;
        }
    }

    Some(total_increase / dt)
}

/// Compute irate: rate between the last two samples.
pub fn compute_irate(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let prev = &samples[samples.len() - 2];
    let last = &samples[samples.len() - 1];
    let dt = last.0 - prev.0;
    if dt <= 0.0 {
        return None;
    }
    let delta = if last.1 >= prev.1 {
        last.1 - prev.1
    } else {
        last.1 // counter reset
    };
    Some(delta / dt)
}

/// Compute increase: total increase over the range (rate * duration).
pub fn compute_increase(samples: &[(f64, f64)]) -> Option<f64> {
    let rate = compute_rate(samples)?;
    let first = samples.first().unwrap();
    let last = samples.last().unwrap();
    Some(rate * (last.0 - first.0))
}

/// Apply aggregation across multiple series, grouping by the specified labels.
pub fn aggregate_series(
    series: Vec<TimeSeries>,
    op: AggOp,
    by_labels: &[String],
    without: bool,
    step_timestamps: &[f64],
) -> Vec<TimeSeries> {
    // Build group keys
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<&TimeSeries>> = BTreeMap::new();

    for ts in &series {
        let group_key = if without {
            ts.labels
                .iter()
                .filter(|(k, _)| !by_labels.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else if by_labels.is_empty() {
            // No grouping — all series in one group
            BTreeMap::new()
        } else {
            by_labels
                .iter()
                .filter_map(|l| ts.labels.get(l).map(|v| (l.clone(), v.clone())))
                .collect()
        };
        groups.entry(group_key).or_default().push(ts);
    }

    groups
        .into_iter()
        .map(|(group_labels, members)| {
            let samples: Vec<(f64, f64)> = step_timestamps
                .iter()
                .filter_map(|&t| {
                    let values: Vec<f64> = members
                        .iter()
                        .filter_map(|ts| {
                            // Find sample closest to this timestamp
                            ts.samples
                                .iter()
                                .rev()
                                .find(|(st, _)| *st <= t + 30.0) // allow 30s tolerance
                                .map(|(_, v)| *v)
                        })
                        .collect();

                    if values.is_empty() {
                        return None;
                    }

                    let result = match op {
                        AggOp::Sum => values.iter().sum(),
                        AggOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
                        AggOp::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
                        AggOp::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                        AggOp::Count => values.len() as f64,
                    };

                    Some((t, result))
                })
                .collect();

            TimeSeries {
                labels: group_labels,
                samples,
            }
        })
        .collect()
}

/// Build ClickHouse WHERE clause fragments from label matchers.
pub fn matchers_to_sql(matchers: &[LabelMatcher]) -> Vec<String> {
    let mut conditions = Vec::new();
    for m in matchers {
        // Map well-known labels to columns, everything else to Attributes map
        let col = match m.name.as_str() {
            "__name__" => "MetricName".to_string(),
            "service_name" | "job" => "ServiceName".to_string(),
            _ => format!("Attributes['{}']", m.name.replace('\'', "\\'")),
        };

        let escaped = m.value.replace('\'', "\\'");
        let cond = match m.op {
            MatchOp::Eq => format!("{col} = '{escaped}'"),
            MatchOp::Neq => format!("{col} != '{escaped}'"),
            MatchOp::Re => format!("match({col}, '{escaped}')"),
            MatchOp::Nre => format!("NOT match({col}, '{escaped}')"),
        };
        conditions.push(cond);
    }
    conditions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_selector() {
        let expr = parse("http_requests_total").unwrap();
        match expr {
            PromExpr::Selector(s) => {
                assert_eq!(s.name, "http_requests_total");
                assert!(s.matchers.is_empty());
            }
            _ => panic!("expected selector"),
        }
    }

    #[test]
    fn parse_selector_with_matchers() {
        let expr = parse(r#"http_requests_total{service_name="gateway", method="POST"}"#).unwrap();
        match expr {
            PromExpr::Selector(s) => {
                assert_eq!(s.name, "http_requests_total");
                assert_eq!(s.matchers.len(), 2);
                assert_eq!(s.matchers[0].name, "service_name");
                assert_eq!(s.matchers[0].op, MatchOp::Eq);
                assert_eq!(s.matchers[0].value, "gateway");
            }
            _ => panic!("expected selector"),
        }
    }

    #[test]
    fn parse_rate() {
        let expr = parse("rate(http_requests_total[5m])").unwrap();
        match expr {
            PromExpr::RangeFunction { func, selector, range_secs } => {
                assert!(matches!(func, RangeFunc::Rate));
                assert_eq!(selector.name, "http_requests_total");
                assert_eq!(range_secs, 300.0);
            }
            _ => panic!("expected range function"),
        }
    }

    #[test]
    fn parse_sum_by_rate() {
        let expr =
            parse(r#"sum by (service_name) (rate(http_requests_total[5m]))"#).unwrap();
        match expr {
            PromExpr::Aggregation { op, by_labels, .. } => {
                assert!(matches!(op, AggOp::Sum));
                assert_eq!(by_labels, vec!["service_name"]);
            }
            _ => panic!("expected aggregation"),
        }
    }

    #[test]
    fn parse_duration_values() {
        assert_eq!(parse_duration("5m").unwrap(), 300.0);
        assert_eq!(parse_duration("1h").unwrap(), 3600.0);
        assert_eq!(parse_duration("30s").unwrap(), 30.0);
        assert_eq!(parse_duration("1h30m").unwrap(), 5400.0);
    }
}
