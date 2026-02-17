use std::collections::BTreeMap;
use promql_parser::parser::token::{self, TokenType};
use promql_parser::parser::{BinModifier, LabelModifier, VectorMatchCardinality};
use super::types::TimeSeries;

/// Apply a binary operation between two sets of time series.
/// Handles arithmetic (+, -, *, /, %, ^), comparison (==, !=, <, >, <=, >=),
/// and set operations (and, or, unless).
pub fn apply_binary_op(
    op: TokenType,
    lhs: Vec<TimeSeries>,
    rhs: Vec<TimeSeries>,
    modifier: &Option<BinModifier>,
    step_timestamps: &[f64],
) -> Vec<TimeSeries> {
    let return_bool = modifier.as_ref().map_or(false, |m| m.return_bool);

    // Set operations: and, or, unless
    let id = op.id();
    if id == token::T_LAND || id == token::T_LOR || id == token::T_LUNLESS {
        return apply_set_op(op, lhs, rhs, modifier, step_timestamps);
    }

    // Scalar on both sides (single series, single sample each)
    if is_scalar_result(&lhs) && is_scalar_result(&rhs) {
        return apply_scalar_binary(op, &lhs, &rhs, return_bool, step_timestamps);
    }

    // Scalar on one side: apply to each series on the other
    if is_scalar_result(&rhs) {
        return apply_vector_scalar(op, lhs, &rhs, return_bool, false, step_timestamps);
    }
    if is_scalar_result(&lhs) {
        return apply_vector_scalar(op, rhs, &lhs, return_bool, true, step_timestamps);
    }

    // Vector-vector matching
    apply_vector_vector(op, lhs, rhs, modifier, return_bool, step_timestamps)
}

fn is_scalar_result(series: &[TimeSeries]) -> bool {
    series.len() == 1 && series[0].labels.is_empty()
}

/// Both sides are scalars — produce a single scalar result.
fn apply_scalar_binary(
    op: TokenType,
    lhs: &[TimeSeries],
    rhs: &[TimeSeries],
    return_bool: bool,
    step_timestamps: &[f64],
) -> Vec<TimeSeries> {
    let lhs_map = sample_map(&lhs[0], step_timestamps);
    let rhs_map = sample_map(&rhs[0], step_timestamps);

    let samples: Vec<(f64, f64)> = step_timestamps
        .iter()
        .filter_map(|&t| {
            let l = lhs_map.get(&ordered_float(t))?;
            let r = rhs_map.get(&ordered_float(t))?;
            eval_binary_op(op, *l, *r, return_bool).map(|v| (t, v))
        })
        .collect();

    vec![TimeSeries {
        labels: BTreeMap::new(),
        samples,
    }]
}

/// One side is a vector, the other is a scalar.
fn apply_vector_scalar(
    op: TokenType,
    vector: Vec<TimeSeries>,
    scalar: &[TimeSeries],
    return_bool: bool,
    scalar_on_lhs: bool,
    step_timestamps: &[f64],
) -> Vec<TimeSeries> {
    let scalar_map = sample_map(&scalar[0], step_timestamps);

    vector
        .into_iter()
        .map(|ts| {
            let samples: Vec<(f64, f64)> = ts
                .samples
                .iter()
                .filter_map(|&(t, v)| {
                    let s = scalar_map.get(&ordered_float(t)).copied().unwrap_or(f64::NAN);
                    let (l, r) = if scalar_on_lhs { (s, v) } else { (v, s) };
                    eval_binary_op(op, l, r, return_bool).map(|result| (t, result))
                })
                .collect();
            TimeSeries {
                labels: ts.labels,
                samples,
            }
        })
        .filter(|ts| !ts.samples.is_empty())
        .collect()
}

/// Vector-vector binary operation with label matching.
fn apply_vector_vector(
    op: TokenType,
    lhs: Vec<TimeSeries>,
    rhs: Vec<TimeSeries>,
    modifier: &Option<BinModifier>,
    return_bool: bool,
    step_timestamps: &[f64],
) -> Vec<TimeSeries> {
    let (match_labels, is_on) = match modifier {
        Some(m) => match &m.matching {
            Some(LabelModifier::Include(labels)) => (labels.labels.clone(), true),
            Some(LabelModifier::Exclude(labels)) => (labels.labels.clone(), false),
            None => (vec![], false),
        },
        None => (vec![], false),
    };

    let card = modifier
        .as_ref()
        .map(|m| &m.card)
        .cloned()
        .unwrap_or(VectorMatchCardinality::OneToOne);

    // Build a signature → series index for the RHS
    let mut rhs_by_sig: BTreeMap<BTreeMap<String, String>, Vec<usize>> = BTreeMap::new();
    for (i, ts) in rhs.iter().enumerate() {
        let sig = match_signature(&ts.labels, &match_labels, is_on);
        rhs_by_sig.entry(sig).or_default().push(i);
    }

    let mut results = Vec::new();

    for lts in &lhs {
        let sig = match_signature(&lts.labels, &match_labels, is_on);
        let rhs_indices = match rhs_by_sig.get(&sig) {
            Some(indices) => indices,
            None => continue,
        };

        for &ri in rhs_indices {
            let rts = &rhs[ri];
            let lhs_map = sample_map(lts, step_timestamps);
            let rhs_map = sample_map(rts, step_timestamps);

            let samples: Vec<(f64, f64)> = step_timestamps
                .iter()
                .filter_map(|&t| {
                    let key = ordered_float(t);
                    let l = lhs_map.get(&key)?;
                    let r = rhs_map.get(&key)?;
                    eval_binary_op(op, *l, *r, return_bool).map(|v| (t, v))
                })
                .collect();

            if samples.is_empty() {
                continue;
            }

            // Determine output labels based on cardinality
            let output_labels = match &card {
                VectorMatchCardinality::OneToOne => {
                    if is_on {
                        // Keep only the match labels from LHS
                        lts.labels
                            .iter()
                            .filter(|(k, _)| match_labels.contains(k))
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect()
                    } else if !match_labels.is_empty() {
                        // ignoring: keep all LHS labels except the ignored ones
                        lts.labels
                            .iter()
                            .filter(|(k, _)| !match_labels.contains(k))
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect()
                    } else {
                        // No modifier: match on all labels, keep LHS labels
                        lts.labels.clone()
                    }
                }
                VectorMatchCardinality::ManyToOne(extra) => {
                    // group_left: keep LHS labels + extra from RHS
                    let mut labels = lts.labels.clone();
                    for extra_label in &extra.labels {
                        if let Some(v) = rts.labels.get(extra_label) {
                            labels.insert(extra_label.clone(), v.clone());
                        }
                    }
                    labels
                }
                VectorMatchCardinality::OneToMany(extra) => {
                    // group_right: keep RHS labels + extra from LHS
                    let mut labels = rts.labels.clone();
                    for extra_label in &extra.labels {
                        if let Some(v) = lts.labels.get(extra_label) {
                            labels.insert(extra_label.clone(), v.clone());
                        }
                    }
                    labels
                }
                VectorMatchCardinality::ManyToMany => lts.labels.clone(),
            };

            results.push(TimeSeries {
                labels: output_labels,
                samples,
            });
        }
    }

    results
}

/// Set operations: and, or, unless.
fn apply_set_op(
    op: TokenType,
    lhs: Vec<TimeSeries>,
    rhs: Vec<TimeSeries>,
    modifier: &Option<BinModifier>,
    _step_timestamps: &[f64],
) -> Vec<TimeSeries> {
    let (match_labels, is_on) = match modifier {
        Some(m) => match &m.matching {
            Some(LabelModifier::Include(labels)) => (labels.labels.clone(), true),
            Some(LabelModifier::Exclude(labels)) => (labels.labels.clone(), false),
            None => (vec![], false),
        },
        None => (vec![], false),
    };

    // Build RHS signature set
    let rhs_sigs: std::collections::HashSet<BTreeMap<String, String>> = rhs
        .iter()
        .map(|ts| match_signature(&ts.labels, &match_labels, is_on))
        .collect();

    let id = op.id();
    if id == token::T_LAND {
        // `and`: keep LHS series that have a match in RHS
        lhs.into_iter()
            .filter(|ts| {
                let sig = match_signature(&ts.labels, &match_labels, is_on);
                rhs_sigs.contains(&sig)
            })
            .collect()
    } else if id == token::T_LUNLESS {
        // `unless`: keep LHS series that do NOT have a match in RHS
        lhs.into_iter()
            .filter(|ts| {
                let sig = match_signature(&ts.labels, &match_labels, is_on);
                !rhs_sigs.contains(&sig)
            })
            .collect()
    } else if id == token::T_LOR {
        // `or`: all LHS series, plus RHS series without a match in LHS
        let lhs_sigs: std::collections::HashSet<BTreeMap<String, String>> = lhs
            .iter()
            .map(|ts| match_signature(&ts.labels, &match_labels, is_on))
            .collect();

        let mut result = lhs;
        for ts in rhs {
            let sig = match_signature(&ts.labels, &match_labels, is_on);
            if !lhs_sigs.contains(&sig) {
                result.push(ts);
            }
        }
        result
    } else {
        vec![]
    }
}

/// Build a match signature from a label set, used for vector matching.
fn match_signature(
    labels: &BTreeMap<String, String>,
    match_labels: &[String],
    is_on: bool,
) -> BTreeMap<String, String> {
    if is_on {
        // on(labels): match only on specified labels
        match_labels
            .iter()
            .filter_map(|l| labels.get(l).map(|v| (l.clone(), v.clone())))
            .collect()
    } else if !match_labels.is_empty() {
        // ignoring(labels): match on all labels except specified
        labels
            .iter()
            .filter(|(k, _)| !match_labels.contains(k) && k.as_str() != "__name__")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    } else {
        // Default: match on all labels (excluding __name__ for binary ops)
        labels
            .iter()
            .filter(|(k, _)| k.as_str() != "__name__")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

/// Build a timestamp → value map for a series, snapping to step timestamps.
fn sample_map(ts: &TimeSeries, step_timestamps: &[f64]) -> BTreeMap<i64, f64> {
    let half_step = if step_timestamps.len() >= 2 {
        (step_timestamps[1] - step_timestamps[0]) / 2.0
    } else {
        5.0
    };

    let mut map = BTreeMap::new();
    for &t in step_timestamps {
        if let Some((_, v)) = ts
            .samples
            .iter()
            .rev()
            .find(|(st, _)| *st <= t + half_step && *st >= t - half_step)
        {
            map.insert(ordered_float(t), *v);
        }
    }
    map
}

/// Convert f64 to i64 key for BTreeMap (multiply by 1000 for ms precision).
fn ordered_float(f: f64) -> i64 {
    (f * 1000.0) as i64
}

/// Evaluate a single binary operation between two scalar values.
fn eval_binary_op(op: TokenType, l: f64, r: f64, return_bool: bool) -> Option<f64> {
    let id = op.id();
    // Arithmetic
    if id == token::T_ADD { return Some(l + r); }
    if id == token::T_SUB { return Some(l - r); }
    if id == token::T_MUL { return Some(l * r); }
    if id == token::T_DIV {
        return if r == 0.0 { Some(f64::NAN) } else { Some(l / r) };
    }
    if id == token::T_MOD {
        return if r == 0.0 { Some(f64::NAN) } else { Some(l % r) };
    }
    if id == token::T_POW { return Some(l.powf(r)); }
    // Comparison
    if id == token::T_EQLC { return comparison_op(l == r, l, return_bool); }
    if id == token::T_NEQ { return comparison_op(l != r, l, return_bool); }
    if id == token::T_LSS { return comparison_op(l < r, l, return_bool); }
    if id == token::T_GTR { return comparison_op(l > r, l, return_bool); }
    if id == token::T_LTE { return comparison_op(l <= r, l, return_bool); }
    if id == token::T_GTE { return comparison_op(l >= r, l, return_bool); }
    None
}

fn comparison_op(cond: bool, value: f64, return_bool: bool) -> Option<f64> {
    if return_bool {
        Some(if cond { 1.0 } else { 0.0 })
    } else if cond {
        Some(value)
    } else {
        None // filter out
    }
}
