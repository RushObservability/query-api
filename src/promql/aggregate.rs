use std::collections::BTreeMap;
use super::compute::quantile_sorted;
use super::types::{AggOp, TimeSeries};

// ═══════════════════════════════════════════════════════════════════
// Aggregation
// ═══════════════════════════════════════════════════════════════════

/// Apply aggregation across multiple series, grouping by the specified labels.
pub fn aggregate_series(
    series: Vec<TimeSeries>,
    op: AggOp,
    by_labels: &[String],
    without: bool,
    step_timestamps: &[f64],
    param: Option<f64>,
) -> Vec<TimeSeries> {
    // topk/bottomk are special: they select series rather than combining values
    if matches!(op, AggOp::Topk | AggOp::Bottomk) {
        return aggregate_topk_bottomk(series, op, param, by_labels, without);
    }

    // Build group keys
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<&TimeSeries>> = BTreeMap::new();

    for ts in &series {
        let group_key = build_group_key(&ts.labels, by_labels, without);
        groups.entry(group_key).or_default().push(ts);
    }

    groups
        .into_iter()
        .map(|(group_labels, members)| {
            let samples: Vec<(f64, f64)> = step_timestamps
                .iter()
                .filter_map(|&t| {
                    // Find the value at (or nearest before) step time t.
                    // Use half the step interval as tolerance, minimum 1s.
                    let half_step = if step_timestamps.len() >= 2 {
                        (step_timestamps[1] - step_timestamps[0]) / 2.0
                    } else {
                        5.0
                    };
                    let values: Vec<f64> = members
                        .iter()
                        .filter_map(|ts| {
                            ts.samples
                                .iter()
                                .rev()
                                .find(|(st, _)| *st <= t + half_step && *st >= t - half_step)
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
                        AggOp::Stddev => {
                            let mean = values.iter().sum::<f64>() / values.len() as f64;
                            let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                                / values.len() as f64;
                            var.sqrt()
                        }
                        AggOp::Stdvar => {
                            let mean = values.iter().sum::<f64>() / values.len() as f64;
                            values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                                / values.len() as f64
                        }
                        AggOp::Quantile => {
                            let q = param.unwrap_or(0.5);
                            let mut sorted = values.clone();
                            sorted.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            quantile_sorted(&sorted, q)
                        }
                        AggOp::Group => 1.0,
                        AggOp::CountValues => {
                            // Count distinct values (simplified — returns count of unique values)
                            let mut unique: Vec<i64> = values
                                .iter()
                                .map(|v| (*v * 1_000_000.0) as i64)
                                .collect();
                            unique.sort();
                            unique.dedup();
                            unique.len() as f64
                        }
                        AggOp::Topk | AggOp::Bottomk => unreachable!(),
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

pub fn build_group_key(
    labels: &BTreeMap<String, String>,
    by_labels: &[String],
    without: bool,
) -> BTreeMap<String, String> {
    if without {
        labels
            .iter()
            .filter(|(k, _)| !by_labels.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    } else if by_labels.is_empty() {
        BTreeMap::new()
    } else {
        by_labels
            .iter()
            .filter_map(|l| labels.get(l).map(|v| (l.clone(), v.clone())))
            .collect()
    }
}

/// Select top-k or bottom-k series by their latest value.
fn aggregate_topk_bottomk(
    series: Vec<TimeSeries>,
    op: AggOp,
    param: Option<f64>,
    by_labels: &[String],
    without: bool,
) -> Vec<TimeSeries> {
    let k = param.unwrap_or(5.0) as usize;
    if k == 0 {
        return vec![];
    }

    // Group series
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<TimeSeries>> = BTreeMap::new();
    for ts in series {
        let group_key = build_group_key(&ts.labels, by_labels, without);
        groups.entry(group_key).or_default().push(ts);
    }

    let mut result = Vec::new();
    for (_, mut members) in groups {
        // Sort by latest value
        members.sort_by(|a, b| {
            let a_val = a.samples.last().map(|(_, v)| *v).unwrap_or(0.0);
            let b_val = b.samples.last().map(|(_, v)| *v).unwrap_or(0.0);
            a_val.partial_cmp(&b_val).unwrap_or(std::cmp::Ordering::Equal)
        });

        let selected: Vec<TimeSeries> = match op {
            AggOp::Topk => members.into_iter().rev().take(k).collect(),
            AggOp::Bottomk => members.into_iter().take(k).collect(),
            _ => unreachable!(),
        };
        result.extend(selected);
    }

    result
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_approx(actual: f64, expected: f64, epsilon: f64) {
        assert!(
            (actual - expected).abs() < epsilon,
            "expected {expected}, got {actual} (diff: {})",
            (actual - expected).abs()
        );
    }

    #[test]
    fn test_aggregate_sum() {
        let series = vec![
            TimeSeries {
                labels: [("__name__".into(), "m".into())].into(),
                samples: vec![(10.0, 5.0), (20.0, 10.0)],
            },
            TimeSeries {
                labels: [("__name__".into(), "m".into())].into(),
                samples: vec![(10.0, 3.0), (20.0, 7.0)],
            },
        ];
        let result = aggregate_series(series, AggOp::Sum, &[], false, &[10.0, 20.0], None);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].samples.len(), 2);
        assert_approx(result[0].samples[0].1, 8.0, 0.001);
        assert_approx(result[0].samples[1].1, 17.0, 0.001);
    }

    #[test]
    fn test_aggregate_avg() {
        let series = vec![
            TimeSeries {
                labels: BTreeMap::new(),
                samples: vec![(10.0, 10.0)],
            },
            TimeSeries {
                labels: BTreeMap::new(),
                samples: vec![(10.0, 20.0)],
            },
            TimeSeries {
                labels: BTreeMap::new(),
                samples: vec![(10.0, 30.0)],
            },
        ];
        let result = aggregate_series(series, AggOp::Avg, &[], false, &[10.0], None);
        assert_approx(result[0].samples[0].1, 20.0, 0.001);
    }

    #[test]
    fn test_aggregate_stddev() {
        let series = vec![
            TimeSeries {
                labels: BTreeMap::new(),
                samples: vec![(10.0, 10.0)],
            },
            TimeSeries {
                labels: BTreeMap::new(),
                samples: vec![(10.0, 20.0)],
            },
            TimeSeries {
                labels: BTreeMap::new(),
                samples: vec![(10.0, 30.0)],
            },
        ];
        let result = aggregate_series(series, AggOp::Stddev, &[], false, &[10.0], None);
        assert_approx(result[0].samples[0].1, 8.165, 0.01);
    }

    #[test]
    fn test_aggregate_quantile() {
        let series: Vec<TimeSeries> = (0..10)
            .map(|i| TimeSeries {
                labels: BTreeMap::new(),
                samples: vec![(10.0, i as f64 * 10.0)],
            })
            .collect();
        let result =
            aggregate_series(series, AggOp::Quantile, &[], false, &[10.0], Some(0.5));
        assert_approx(result[0].samples[0].1, 45.0, 0.1);
    }

    #[test]
    fn test_aggregate_group() {
        let series = vec![
            TimeSeries {
                labels: BTreeMap::new(),
                samples: vec![(10.0, 42.0)],
            },
            TimeSeries {
                labels: BTreeMap::new(),
                samples: vec![(10.0, 99.0)],
            },
        ];
        let result = aggregate_series(series, AggOp::Group, &[], false, &[10.0], None);
        assert_approx(result[0].samples[0].1, 1.0, 0.001);
    }

    #[test]
    fn test_topk() {
        let series = vec![
            TimeSeries {
                labels: [("instance".into(), "a".into())].into(),
                samples: vec![(10.0, 100.0)],
            },
            TimeSeries {
                labels: [("instance".into(), "b".into())].into(),
                samples: vec![(10.0, 300.0)],
            },
            TimeSeries {
                labels: [("instance".into(), "c".into())].into(),
                samples: vec![(10.0, 200.0)],
            },
        ];
        let result = aggregate_series(series, AggOp::Topk, &[], false, &[10.0], Some(2.0));
        assert_eq!(result.len(), 2);
        let values: Vec<f64> = result.iter().map(|s| s.samples[0].1).collect();
        assert!(values.contains(&300.0));
        assert!(values.contains(&200.0));
    }

    #[test]
    fn test_bottomk() {
        let series = vec![
            TimeSeries {
                labels: [("instance".into(), "a".into())].into(),
                samples: vec![(10.0, 100.0)],
            },
            TimeSeries {
                labels: [("instance".into(), "b".into())].into(),
                samples: vec![(10.0, 300.0)],
            },
            TimeSeries {
                labels: [("instance".into(), "c".into())].into(),
                samples: vec![(10.0, 200.0)],
            },
        ];
        let result = aggregate_series(series, AggOp::Bottomk, &[], false, &[10.0], Some(2.0));
        assert_eq!(result.len(), 2);
        let values: Vec<f64> = result.iter().map(|s| s.samples[0].1).collect();
        assert!(values.contains(&100.0));
        assert!(values.contains(&200.0));
    }
}
