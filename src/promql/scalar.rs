use super::types::{ScalarFunc, TimeSeries};

// ═══════════════════════════════════════════════════════════════════
// Scalar function application
// ═══════════════════════════════════════════════════════════════════

/// Apply a scalar function to each sample value in each series.
pub fn apply_scalar_func(
    series: Vec<TimeSeries>,
    func: ScalarFunc,
    extra_args: &[f64],
) -> Vec<TimeSeries> {
    // histogram_quantile is handled separately
    if func == ScalarFunc::HistogramQuantile {
        let phi = extra_args.first().copied().unwrap_or(0.5);
        return compute_histogram_quantile(phi, &series);
    }

    series
        .into_iter()
        .map(|mut ts| {
            ts.samples = ts
                .samples
                .into_iter()
                .map(|(t, v)| {
                    let new_v = apply_scalar_op(func, v, extra_args);
                    (t, new_v)
                })
                .collect();
            ts
        })
        .collect()
}

pub fn apply_scalar_op(func: ScalarFunc, v: f64, args: &[f64]) -> f64 {
    match func {
        ScalarFunc::Abs => v.abs(),
        ScalarFunc::Ceil => v.ceil(),
        ScalarFunc::Floor => v.floor(),
        ScalarFunc::Round => {
            let to = args.first().copied().unwrap_or(1.0);
            if to == 0.0 { v } else { (v / to).round() * to }
        }
        ScalarFunc::Sqrt => v.sqrt(),
        ScalarFunc::Exp => v.exp(),
        ScalarFunc::Ln => v.ln(),
        ScalarFunc::Log2 => v.log2(),
        ScalarFunc::Log10 => v.log10(),
        ScalarFunc::Sgn => {
            if v > 0.0 {
                1.0
            } else if v < 0.0 {
                -1.0
            } else {
                0.0
            }
        }
        ScalarFunc::ClampMin => {
            let min = args.first().copied().unwrap_or(f64::NEG_INFINITY);
            v.max(min)
        }
        ScalarFunc::ClampMax => {
            let max = args.first().copied().unwrap_or(f64::INFINITY);
            v.min(max)
        }
        ScalarFunc::Clamp => {
            let min = args.first().copied().unwrap_or(f64::NEG_INFINITY);
            let max = args.get(1).copied().unwrap_or(f64::INFINITY);
            v.clamp(min, max)
        }
        ScalarFunc::Sin => v.sin(),
        ScalarFunc::Cos => v.cos(),
        ScalarFunc::Asin => v.asin(),
        ScalarFunc::Acos => v.acos(),
        ScalarFunc::Atan2 => {
            let other = args.first().copied().unwrap_or(0.0);
            v.atan2(other)
        }
        ScalarFunc::Sinh => v.sinh(),
        ScalarFunc::Cosh => v.cosh(),
        ScalarFunc::Asinh => v.asinh(),
        ScalarFunc::Acosh => v.acosh(),
        ScalarFunc::Atanh => v.atanh(),
        ScalarFunc::Deg => v.to_degrees(),
        ScalarFunc::Rad => v.to_radians(),
        ScalarFunc::Pi => std::f64::consts::PI,
        ScalarFunc::Timestamp => v, // pass-through (time is the timestamp)
        ScalarFunc::HistogramQuantile => v, // handled separately
    }
}

/// Compute histogram_quantile from a set of histogram bucket series.
/// Groups buckets by labels and evaluation timestamp. Never carry a bucket from
/// one step into another: missing data must not become a fabricated observation.
fn compute_histogram_quantile(phi: f64, series: &[TimeSeries]) -> Vec<TimeSeries> {
    use std::collections::BTreeMap;

    // Each sample is (timestamp, upper bound, cumulative count).
    type BucketSamples = Vec<(f64, f64, f64)>;
    let mut groups: BTreeMap<BTreeMap<String, String>, BucketSamples> = BTreeMap::new();

    for ts in series {
        let mut group_labels = ts.labels.clone();
        let Some(le_str) = group_labels.remove("le") else {
            continue;
        };
        group_labels.remove("__name__");

        let Ok(le) = le_str.parse::<f64>() else {
            continue;
        };
        if le.is_nan() || ts.samples.is_empty() {
            continue;
        }
        groups.entry(group_labels).or_default().extend(
            ts.samples
                .iter()
                .map(|&(timestamp, count)| (timestamp, le, count)),
        );
    }

    groups
        .into_iter()
        .map(|(labels, mut points)| {
            points.sort_by(|a, b| a.0.total_cmp(&b.0));
            let samples = points
                .chunk_by(|a, b| a.0 == b.0)
                .map(|step| {
                    let buckets = step.iter().map(|&(_, le, count)| (le, count)).collect();
                    (step[0].0, classic_bucket_quantile(phi, buckets))
                })
                .collect();
            TimeSeries { labels, samples }
        })
        .collect()
}

/// Classic cumulative histogram semantics documented by Prometheus:
/// https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile
fn classic_bucket_quantile(phi: f64, mut buckets: Vec<(f64, f64)>) -> f64 {
    if phi.is_nan() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    if buckets.last().is_none_or(|b| b.0 != f64::INFINITY) {
        return f64::NAN;
    }
    // Different string representations of the same bound are one bucket.
    let mut merged: Vec<(f64, f64)> = Vec::with_capacity(buckets.len());
    for (bound, count) in buckets {
        if let Some(last) = merged.last_mut().filter(|last| last.0 == bound) {
            last.1 += count;
        } else {
            merged.push((bound, count));
        }
    }
    if merged.len() < 2 {
        return f64::NAN;
    }
    // Ignore relative rounding differences up to 1e-12 and repair decreases
    // in cumulative counts, as Prometheus does before interpolation.
    for i in 1..merged.len() {
        let previous = merged[i - 1].1;
        let current = merged[i].1;
        let close = previous.is_finite()
            && current.is_finite()
            && (current - previous).abs() <= 1e-12 * (current.abs() + previous.abs());
        if current < previous || close {
            merged[i].1 = previous;
        }
    }
    let total = merged.last().unwrap().1;
    if total == 0.0 || total.is_nan() {
        return f64::NAN;
    }
    let target = phi * total;
    for (i, &(upper, count)) in merged[..merged.len() - 1].iter().enumerate() {
        if count >= target {
            if i == 0 && upper <= 0.0 {
                return upper;
            }
            let (lower, previous_count) = if i == 0 { (0.0, 0.0) } else { merged[i - 1] };
            return lower
                + (upper - lower) * ((target - previous_count) / (count - previous_count));
        }
    }
    // A quantile in the unbounded bucket is the highest finite boundary.
    merged[merged.len() - 2].0
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn assert_approx(actual: f64, expected: f64, epsilon: f64) {
        assert!(
            (actual - expected).abs() < epsilon,
            "expected {expected}, got {actual} (diff: {})",
            (actual - expected).abs()
        );
    }

    #[test]
    fn test_scalar_abs() {
        assert_approx(apply_scalar_op(ScalarFunc::Abs, -42.0, &[]), 42.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Abs, 42.0, &[]), 42.0, 0.001);
    }

    #[test]
    fn test_scalar_ceil_floor() {
        assert_approx(apply_scalar_op(ScalarFunc::Ceil, 3.2, &[]), 4.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Floor, 3.8, &[]), 3.0, 0.001);
    }

    #[test]
    fn test_scalar_round() {
        assert_approx(apply_scalar_op(ScalarFunc::Round, 3.456, &[]), 3.0, 0.001);
        assert_approx(
            apply_scalar_op(ScalarFunc::Round, 3.456, &[0.1]),
            3.5,
            0.001,
        );
        assert_approx(
            apply_scalar_op(ScalarFunc::Round, 3.456, &[0.01]),
            3.46,
            0.001,
        );
    }

    #[test]
    fn test_scalar_sqrt() {
        assert_approx(apply_scalar_op(ScalarFunc::Sqrt, 16.0, &[]), 4.0, 0.001);
        assert_approx(
            apply_scalar_op(ScalarFunc::Sqrt, 2.0, &[]),
            std::f64::consts::SQRT_2,
            0.001,
        );
    }

    #[test]
    fn test_scalar_exp_ln() {
        let e = std::f64::consts::E;
        assert_approx(apply_scalar_op(ScalarFunc::Exp, 1.0, &[]), e, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Ln, e, &[]), 1.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Log2, 8.0, &[]), 3.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Log10, 1000.0, &[]), 3.0, 0.001);
    }

    #[test]
    fn test_scalar_sgn() {
        assert_approx(apply_scalar_op(ScalarFunc::Sgn, 42.0, &[]), 1.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Sgn, -42.0, &[]), -1.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Sgn, 0.0, &[]), 0.0, 0.001);
    }

    #[test]
    fn test_scalar_clamp() {
        assert_approx(
            apply_scalar_op(ScalarFunc::ClampMin, -5.0, &[0.0]),
            0.0,
            0.001,
        );
        assert_approx(
            apply_scalar_op(ScalarFunc::ClampMin, 5.0, &[0.0]),
            5.0,
            0.001,
        );
        assert_approx(
            apply_scalar_op(ScalarFunc::ClampMax, 150.0, &[100.0]),
            100.0,
            0.001,
        );
        assert_approx(
            apply_scalar_op(ScalarFunc::ClampMax, 50.0, &[100.0]),
            50.0,
            0.001,
        );
        assert_approx(
            apply_scalar_op(ScalarFunc::Clamp, -5.0, &[0.0, 100.0]),
            0.0,
            0.001,
        );
        assert_approx(
            apply_scalar_op(ScalarFunc::Clamp, 50.0, &[0.0, 100.0]),
            50.0,
            0.001,
        );
        assert_approx(
            apply_scalar_op(ScalarFunc::Clamp, 150.0, &[0.0, 100.0]),
            100.0,
            0.001,
        );
    }

    #[test]
    fn test_scalar_trig() {
        assert_approx(apply_scalar_op(ScalarFunc::Sin, 0.0, &[]), 0.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Cos, 0.0, &[]), 1.0, 0.001);
        assert_approx(
            apply_scalar_op(ScalarFunc::Sin, std::f64::consts::FRAC_PI_2, &[]),
            1.0,
            0.001,
        );
        assert_approx(
            apply_scalar_op(ScalarFunc::Deg, std::f64::consts::PI, &[]),
            180.0,
            0.001,
        );
        assert_approx(
            apply_scalar_op(ScalarFunc::Rad, 180.0, &[]),
            std::f64::consts::PI,
            0.001,
        );
    }

    #[test]
    fn test_apply_scalar_func_to_series() {
        let series = vec![TimeSeries {
            labels: BTreeMap::new(),
            samples: vec![(10.0, -5.0), (20.0, 3.0), (30.0, -8.0)],
        }];
        let result = apply_scalar_func(series, ScalarFunc::Abs, &[]);
        assert_eq!(result.len(), 1);
        assert_approx(result[0].samples[0].1, 5.0, 0.001);
        assert_approx(result[0].samples[1].1, 3.0, 0.001);
        assert_approx(result[0].samples[2].1, 8.0, 0.001);
    }

    #[test]
    fn test_apply_clamp_min_to_series() {
        let series = vec![TimeSeries {
            labels: BTreeMap::new(),
            samples: vec![(10.0, -5.0), (20.0, 3.0), (30.0, -8.0)],
        }];
        let result = apply_scalar_func(series, ScalarFunc::ClampMin, &[0.0]);
        assert_approx(result[0].samples[0].1, 0.0, 0.001);
        assert_approx(result[0].samples[1].1, 3.0, 0.001);
        assert_approx(result[0].samples[2].1, 0.0, 0.001);
    }

    // ── Additional VictoriaMetrics-style scalar identities ──

    #[test]
    fn test_scalar_pi_exp0_ln1() {
        assert_approx(
            apply_scalar_op(ScalarFunc::Pi, 0.0, &[]),
            std::f64::consts::PI,
            1e-12,
        );
        assert_approx(apply_scalar_op(ScalarFunc::Exp, 0.0, &[]), 1.0, 1e-12);
        assert_approx(apply_scalar_op(ScalarFunc::Ln, 1.0, &[]), 0.0, 1e-12);
    }

    #[test]
    fn test_scalar_ceil_floor_vm_cases() {
        // VM TestExecSuccess: ceil(1.2)=2, floor(1.8)=1.
        assert_approx(apply_scalar_op(ScalarFunc::Ceil, 1.2, &[]), 2.0, 1e-12);
        assert_approx(apply_scalar_op(ScalarFunc::Floor, 1.8, &[]), 1.0, 1e-12);
    }

    #[test]
    fn test_scalar_round_to_tenth() {
        // round(2.34, 0.1) ≈ 2.3
        assert_approx(apply_scalar_op(ScalarFunc::Round, 2.34, &[0.1]), 2.3, 1e-9);
    }

    #[test]
    fn test_scalar_deg_rad_roundtrip() {
        // rad(deg(x)) == x for an arbitrary angle.
        let x = 1.234_f64;
        let deg = apply_scalar_op(ScalarFunc::Deg, x, &[]);
        let back = apply_scalar_op(ScalarFunc::Rad, deg, &[]);
        assert_approx(back, x, 1e-12);
    }

    #[test]
    fn test_apply_scalar_func_abs_over_two_sample_series() {
        // Abs over [(0,-1),(60,-2)] → [(0,1),(60,2)]
        let series = vec![TimeSeries {
            labels: BTreeMap::new(),
            samples: vec![(0.0, -1.0), (60.0, -2.0)],
        }];
        let result = apply_scalar_func(series, ScalarFunc::Abs, &[]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].samples[0], (0.0, 1.0));
        assert_eq!(result[0].samples[1], (60.0, 2.0));
    }

    // ── histogram_quantile ──
    //
    // Classic cumulative buckets with the same non-le labels.
    // Counts: le 0.1→1, 0.5→2, 1→5, +Inf→10 (total 10).
    //
    // Interpolation (mirroring compute_histogram_quantile): target = phi*total; find the
    // first bucket whose cumulative count >= target, then linearly interpolate within it:
    //   result = prev_le + (le - prev_le) * (target - prev_count) / (count - prev_count)

    fn bucket(le: &str, count: f64) -> TimeSeries {
        TimeSeries {
            labels: [
                (
                    "__name__".into(),
                    "http_request_duration_seconds_bucket".into(),
                ),
                ("le".into(), le.into()),
            ]
            .into(),
            samples: vec![(100.0, count)],
        }
    }

    fn classic_buckets() -> Vec<TimeSeries> {
        vec![
            bucket("0.1", 1.0),
            bucket("0.5", 2.0),
            bucket("1", 5.0),
            bucket("+Inf", 10.0),
        ]
    }

    #[test]
    fn test_histogram_quantile_p50() {
        // phi=0.5 → target=5. First cumulative count >= 5 is le=1 (count 5).
        // prev=(le 0.5, count 2). bucket_count=5-2=3. fraction=(5-2)/3=1.0.
        // result = 0.5 + (1 - 0.5)*1.0 = 1.0.
        let result = apply_scalar_func(classic_buckets(), ScalarFunc::HistogramQuantile, &[0.5]);
        assert_eq!(result.len(), 1);
        assert!(
            !result[0].labels.contains_key("le"),
            "le label must be dropped"
        );
        assert_approx(result[0].samples[0].1, 1.0, 1e-9);
    }

    #[test]
    fn test_histogram_quantile_finite_interpolation() {
        // phi=0.3 → target=3. First cumulative count >= 3 is le=1 (count 5).
        // prev=(le 0.5, count 2). bucket_count=3. fraction=(3-2)/3=1/3.
        // result = 0.5 + (1 - 0.5)*(1/3) = 0.5 + 0.16666... = 0.66666...
        let result = apply_scalar_func(classic_buckets(), ScalarFunc::HistogramQuantile, &[0.3]);
        assert_approx(result[0].samples[0].1, 0.5 + 0.5 / 3.0, 1e-9);
        // p30 falls between the 0.5 and 1 bucket boundaries.
        assert!(result[0].samples[0].1 > 0.5 && result[0].samples[0].1 < 1.0);
    }

    #[test]
    fn test_histogram_quantile_p90_in_inf_bucket() {
        // Prometheus returns the penultimate boundary for the unbounded bucket.
        let result = apply_scalar_func(classic_buckets(), ScalarFunc::HistogramQuantile, &[0.9]);
        assert_eq!(result[0].samples[0].1, 1.0);
    }

    #[test]
    fn histogram_quantile_evaluates_every_timestamp_and_preserves_groups() {
        let mut series = Vec::new();
        for (service, multiplier) in [("payments", 1.0), ("articles", 2.0)] {
            for (le, values) in [
                ("1", [2.0, 8.0, 0.0]),
                ("2", [10.0, 10.0, 0.0]),
                ("+Inf", [10.0, 10.0, 0.0]),
            ] {
                let mut ts = bucket(le, 0.0);
                ts.labels.insert("service".into(), service.into());
                ts.samples = values
                    .iter()
                    .enumerate()
                    .map(|(i, v)| (100.25 + i as f64 * 15.0, v * multiplier))
                    .collect();
                series.push(ts);
            }
        }
        series.reverse();
        let result = apply_scalar_func(series, ScalarFunc::HistogramQuantile, &[0.5]);
        assert_eq!(result.len(), 2);
        for ts in result {
            assert_eq!(ts.labels.len(), 1);
            assert!(ts.labels.contains_key("service"));
            assert_eq!(ts.samples.len(), 3);
            assert_eq!(ts.samples[0], (100.25, 1.375));
            assert_eq!(ts.samples[1], (115.25, 0.625));
            assert_eq!(ts.samples[2].0, 130.25);
            assert!(ts.samples[2].1.is_nan());
        }
    }

    #[test]
    fn histogram_quantile_never_reuses_buckets_from_other_steps() {
        let mut finite = bucket("1", 10.0);
        finite.samples = vec![(100.0, 10.0), (110.0, 10.0)];
        let mut infinite = bucket("+Inf", 10.0);
        infinite.samples = vec![(100.0, 10.0), (120.0, 10.0)];
        let result = apply_scalar_func(
            vec![finite, infinite],
            ScalarFunc::HistogramQuantile,
            &[0.5],
        );
        assert_eq!(result[0].samples.len(), 3);
        assert_eq!(result[0].samples[0], (100.0, 0.5));
        assert!(result[0].samples[1].1.is_nan());
        assert!(result[0].samples[2].1.is_nan());
    }

    #[test]
    fn histogram_quantile_ignores_invalid_or_missing_bucket_labels() {
        let mut no_le = bucket("1", 99.0);
        no_le.labels.remove("le");
        let result = apply_scalar_func(
            vec![no_le, bucket("bad", 99.0), bucket("NaN", 99.0)],
            ScalarFunc::HistogramQuantile,
            &[0.5],
        );
        assert!(result.is_empty());
        let mut empty = bucket("1", 0.0);
        empty.samples.clear();
        assert!(apply_scalar_func(vec![empty], ScalarFunc::HistogramQuantile, &[0.5]).is_empty());
    }

    #[test]
    fn histogram_quantile_classic_edge_cases() {
        let valid = vec![(1.0, 5.0), (2.0, 10.0), (f64::INFINITY, 10.0)];
        assert_eq!(
            classic_bucket_quantile(-0.1, valid.clone()),
            f64::NEG_INFINITY
        );
        assert_eq!(classic_bucket_quantile(1.1, valid.clone()), f64::INFINITY);
        assert!(classic_bucket_quantile(f64::NAN, valid.clone()).is_nan());
        assert_eq!(classic_bucket_quantile(0.0, valid.clone()), 0.0);
        assert_eq!(classic_bucket_quantile(1.0, valid), 2.0);
        for buckets in [
            vec![],
            vec![(f64::INFINITY, 10.0)],
            vec![(1.0, 10.0)],
            vec![(1.0, 0.0), (f64::INFINITY, 0.0)],
        ] {
            assert!(classic_bucket_quantile(0.5, buckets).is_nan());
        }
        assert_eq!(
            classic_bucket_quantile(0.25, vec![(-2.0, 5.0), (0.0, 10.0), (f64::INFINITY, 10.0)]),
            -2.0
        );
        assert_eq!(
            classic_bucket_quantile(0.75, vec![(-2.0, 5.0), (0.0, 10.0), (f64::INFINITY, 10.0)]),
            -1.0
        );
    }

    #[test]
    fn histogram_quantile_merges_equal_bounds_and_repairs_cumulative_counts() {
        assert_eq!(
            classic_bucket_quantile(
                0.5,
                vec![(f64::INFINITY, 10.0), (1.0, 3.0), (1.0, 2.0), (2.0, 10.0)]
            ),
            1.0
        );
        assert_eq!(
            classic_bucket_quantile(0.9, vec![(1.0, 10.0), (2.0, 5.0), (f64::INFINITY, 10.0)]),
            0.9
        );
        // Tiny increases and decreases must both be treated as rounding noise.
        for delta in [-1e-13, 1e-13] {
            assert_eq!(
                classic_bucket_quantile(
                    1.0,
                    vec![
                        (1.0, 10.0),
                        (2.0, 10.0 + delta),
                        (f64::INFINITY, 10.0 + delta)
                    ]
                ),
                1.0
            );
        }
    }

    #[test]
    fn histogram_quantile_of_aggregated_rates_returns_a_full_curve() {
        use crate::promql::{aggregate::aggregate_series, compute::compute_rate, types::AggOp};

        let steps = [60.0, 90.0, 120.0];
        let mut rates = Vec::new();
        for instance in ["a", "b"] {
            for (le, counts) in [
                ("1", [0.0, 2.0, 4.0, 1.0, 3.0]),
                ("2", [0.0, 10.0, 20.0, 5.0, 15.0]),
                ("+Inf", [0.0, 10.0, 20.0, 5.0, 15.0]),
            ] {
                let mut ts = bucket(le, 0.0);
                ts.labels.insert("instance".into(), instance.into());
                let raw: Vec<_> = counts
                    .iter()
                    .enumerate()
                    .map(|(i, v)| (i as f64 * 30.0, *v))
                    .collect();
                ts.samples = steps
                    .iter()
                    .map(|&t| {
                        let window: Vec<_> = raw
                            .iter()
                            .copied()
                            .filter(|(st, _)| *st > t - 60.0 && *st <= t)
                            .collect();
                        (t, compute_rate(&window).unwrap())
                    })
                    .collect();
                rates.push(ts);
            }
        }
        let sums = aggregate_series(rates, AggOp::Sum, &["le".into()], false, &steps, None);
        let result = apply_scalar_func(sums, ScalarFunc::HistogramQuantile, &[0.95]);
        assert_eq!(result.len(), 1);
        assert!(result[0].labels.is_empty());
        assert_eq!(result[0].samples.len(), steps.len());
        for ((timestamp, value), expected_time) in result[0].samples.iter().zip(steps) {
            assert_eq!(*timestamp, expected_time);
            assert_approx(*value, 1.9375, 1e-9);
        }
    }
}
