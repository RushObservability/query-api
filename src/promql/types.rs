use std::collections::BTreeMap;

// ═══════════════════════════════════════════════════════════════════
// Internal enums (kept for compute/aggregate/scalar dispatch)
// ═══════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RangeFunc {
    // Counter functions
    Rate,
    Irate,
    Increase,
    // Aggregation over time
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    CountOverTime,
    StddevOverTime,
    StdvarOverTime,
    QuantileOverTime,
    LastOverTime,
    FirstOverTime,
    // Gauge functions
    Delta,
    Idelta,
    // Derivative / prediction
    Deriv,
    PredictLinear,
    // Counter analysis
    Changes,
    Resets,
    // Presence
    AbsentOverTime,
    PresentOverTime,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    Quantile,
    Topk,
    Bottomk,
    Group,
    CountValues,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScalarFunc {
    Abs,
    Ceil,
    Floor,
    Round,
    Sqrt,
    Exp,
    Ln,
    Log2,
    Log10,
    Sgn,
    ClampMin,
    ClampMax,
    Clamp,
    HistogramQuantile,
    // Trig
    Sin,
    Cos,
    Asin,
    Acos,
    Atan2,
    Sinh,
    Cosh,
    Asinh,
    Acosh,
    Atanh,
    Deg,
    Rad,
    Pi,
    // Time
    Timestamp,
}

// ═══════════════════════════════════════════════════════════════════
// Evaluation types
// ═══════════════════════════════════════════════════════════════════

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

/// Parse a PromQL duration string like "5m", "1h30m", "30s".
pub fn parse_duration(input: &str) -> Result<f64, String> {
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
