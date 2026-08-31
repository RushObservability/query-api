use super::compute::quantile_sorted;
use super::types::{AggOp, TimeSeries};
use std::collections::BTreeMap;

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
                            let mut unique: Vec<i64> =
                                values.iter().map(|v| (*v * 1_000_000.0) as i64).collect();
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
            a_val
                .partial_cmp(&b_val)
                .unwrap_or(std::cmp::Ordering::Equal)
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
        let result = aggregate_series(series, AggOp::Quantile, &[], false, &[10.0], Some(0.5));
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

    // ── Grouped aggregation with label + value assertions ──
    //
    // Mirrors the structure of VictoriaMetrics' aggregate tests
    // (app/vmselect/promql/exec_test.go, `sum(...) by (...)`/`without(...)` cases):
    // build three labeled series sampled at the SAME timestamps as the step grid so the
    // ±(step/2) window picks each value deterministically, then assert grouped output
    // labels AND values.
    //
    // Series (steps = [0, 60]):
    //   {job:api, inst:a} → (0,10),(60,1)
    //   {job:api, inst:b} → (0,20),(60,2)
    //   {job:db,  inst:c} → (0,40),(60,4)

    fn labeled(job: &str, inst: &str, samples: Vec<(f64, f64)>) -> TimeSeries {
        TimeSeries {
            labels: [("job".into(), job.into()), ("inst".into(), inst.into())].into(),
            samples,
        }
    }

    fn three_series() -> Vec<TimeSeries> {
        vec![
            labeled("api", "a", vec![(0.0, 10.0), (60.0, 1.0)]),
            labeled("api", "b", vec![(0.0, 20.0), (60.0, 2.0)]),
            labeled("db", "c", vec![(0.0, 40.0), (60.0, 4.0)]),
        ]
    }

    const STEPS: [f64; 2] = [0.0, 60.0];

    /// Find the produced series whose group labels equal the given map.
    fn group<'a>(result: &'a [TimeSeries], want: &[(&str, &str)]) -> &'a TimeSeries {
        let want_map: BTreeMap<String, String> = want
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        result
            .iter()
            .find(|s| s.labels == want_map)
            .unwrap_or_else(|| {
                panic!(
                    "no group with labels {want_map:?} in {:?}",
                    result.iter().map(|s| &s.labels).collect::<Vec<_>>()
                )
            })
    }

    #[test]
    fn test_sum_by_job() {
        let by = vec!["job".to_string()];
        let result = aggregate_series(three_series(), AggOp::Sum, &by, false, &STEPS, None);
        assert_eq!(result.len(), 2);
        let api = group(&result, &[("job", "api")]);
        assert_approx(api.samples[0].1, 30.0, 0.001); // 10+20 at t=0
        assert_approx(api.samples[1].1, 3.0, 0.001); // 1+2 at t=60
        let db = group(&result, &[("job", "db")]);
        assert_approx(db.samples[0].1, 40.0, 0.001);
        assert_approx(db.samples[1].1, 4.0, 0.001);
    }

    #[test]
    fn test_avg_by_job() {
        let by = vec!["job".to_string()];
        let result = aggregate_series(three_series(), AggOp::Avg, &by, false, &STEPS, None);
        let api = group(&result, &[("job", "api")]);
        assert_approx(api.samples[0].1, 15.0, 0.001); // (10+20)/2
        let db = group(&result, &[("job", "db")]);
        assert_approx(db.samples[0].1, 40.0, 0.001); // single member
    }

    #[test]
    fn test_min_max_by_job() {
        let by = vec!["job".to_string()];
        let min = aggregate_series(three_series(), AggOp::Min, &by, false, &STEPS, None);
        assert_approx(group(&min, &[("job", "api")]).samples[0].1, 10.0, 0.001);
        let max = aggregate_series(three_series(), AggOp::Max, &by, false, &STEPS, None);
        assert_approx(group(&max, &[("job", "api")]).samples[0].1, 20.0, 0.001);
    }

    #[test]
    fn test_count_by_job() {
        let by = vec!["job".to_string()];
        let result = aggregate_series(three_series(), AggOp::Count, &by, false, &STEPS, None);
        assert_approx(group(&result, &[("job", "api")]).samples[0].1, 2.0, 0.001);
        assert_approx(group(&result, &[("job", "db")]).samples[0].1, 1.0, 0.001);
    }

    #[test]
    fn test_stddev_stdvar_by_job() {
        let by = vec!["job".to_string()];
        // api group values at t=0: [10, 20]. mean=15, var=((25)+(25))/2=25, sd=5.
        let var = aggregate_series(three_series(), AggOp::Stdvar, &by, false, &STEPS, None);
        assert_approx(group(&var, &[("job", "api")]).samples[0].1, 25.0, 0.001);
        let sd = aggregate_series(three_series(), AggOp::Stddev, &by, false, &STEPS, None);
        assert_approx(group(&sd, &[("job", "api")]).samples[0].1, 5.0, 0.001);
    }

    #[test]
    fn test_quantile_by_job() {
        let by = vec!["job".to_string()];
        // api values at t=0: [10,20]. median = linear interp at rank 0.5 → 15.
        let result = aggregate_series(
            three_series(),
            AggOp::Quantile,
            &by,
            false,
            &STEPS,
            Some(0.5),
        );
        assert_approx(group(&result, &[("job", "api")]).samples[0].1, 15.0, 0.001);
    }

    #[test]
    fn test_group_by_job() {
        let by = vec!["job".to_string()];
        let result = aggregate_series(three_series(), AggOp::Group, &by, false, &STEPS, None);
        assert_eq!(result.len(), 2);
        assert_approx(group(&result, &[("job", "api")]).samples[0].1, 1.0, 0.001);
        assert_approx(group(&result, &[("job", "db")]).samples[0].1, 1.0, 0.001);
    }

    #[test]
    fn test_sum_without_inst() {
        // without(inst) drops `inst`, keeps `job` → same grouping as by(job) here,
        // but the group key now also retains any other (here none) labels.
        let without = vec!["inst".to_string()];
        let result = aggregate_series(three_series(), AggOp::Sum, &without, true, &STEPS, None);
        assert_eq!(result.len(), 2);
        let api = group(&result, &[("job", "api")]);
        assert_approx(api.samples[0].1, 30.0, 0.001);
        let db = group(&result, &[("job", "db")]);
        assert_approx(db.samples[0].1, 40.0, 0.001);
    }

    #[test]
    fn test_sum_no_grouping_all_in_one() {
        // No by/without labels → all three series collapse into one empty-labeled group.
        let result = aggregate_series(three_series(), AggOp::Sum, &[], false, &STEPS, None);
        assert_eq!(result.len(), 1);
        assert!(result[0].labels.is_empty());
        assert_approx(result[0].samples[0].1, 70.0, 0.001); // 10+20+40
        assert_approx(result[0].samples[1].1, 7.0, 0.001); // 1+2+4
    }

    #[test]
    fn test_topk_bottomk_by_job() {
        // topk(1) by (job): within each job group keep the single highest-valued series.
        let by = vec!["job".to_string()];
        let top = aggregate_series(three_series(), AggOp::Topk, &by, false, &STEPS, Some(1.0));
        // api group: inst b (last value 2) > inst a (last value 1) → keep b.
        let top_api: Vec<_> = top
            .iter()
            .filter(|s| s.labels.get("job").map(|j| j == "api").unwrap_or(false))
            .collect();
        assert_eq!(top_api.len(), 1);
        assert_eq!(top_api[0].labels.get("inst").unwrap(), "b");

        let bottom = aggregate_series(
            three_series(),
            AggOp::Bottomk,
            &by,
            false,
            &STEPS,
            Some(1.0),
        );
        let bot_api: Vec<_> = bottom
            .iter()
            .filter(|s| s.labels.get("job").map(|j| j == "api").unwrap_or(false))
            .collect();
        assert_eq!(bot_api.len(), 1);
        assert_eq!(bot_api[0].labels.get("inst").unwrap(), "a");
    }

    #[test]
    fn test_build_group_key_modes() {
        let labels: BTreeMap<String, String> =
            [("job".into(), "api".into()), ("inst".into(), "a".into())].into();
        // by(job)
        let k = build_group_key(&labels, &["job".to_string()], false);
        assert_eq!(k, [("job".to_string(), "api".to_string())].into());
        // without(inst)
        let k = build_group_key(&labels, &["inst".to_string()], true);
        assert_eq!(k, [("job".to_string(), "api".to_string())].into());
        // no grouping → empty key
        let k = build_group_key(&labels, &[], false);
        assert!(k.is_empty());
    }
}
