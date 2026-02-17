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
            if to == 0.0 {
                v
            } else {
                (v / to).round() * to
            }
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
/// Groups series by labels (excluding "le"), then interpolates across buckets.
fn compute_histogram_quantile(phi: f64, series: &[TimeSeries]) -> Vec<TimeSeries> {
    use std::collections::BTreeMap;

    // Group series by labels excluding "le" and "__name__"
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<(f64, f64)>> = BTreeMap::new();

    for ts in series {
        let mut group_labels = ts.labels.clone();
        let le_str = group_labels.remove("le").unwrap_or_default();
        group_labels.remove("__name__");

        let le: f64 = if le_str == "+Inf" {
            f64::INFINITY
        } else {
            le_str.parse().unwrap_or(0.0)
        };
        let value = ts.samples.last().map(|(_, v)| *v).unwrap_or(0.0);

        groups.entry(group_labels).or_default().push((le, value));
    }

    groups
        .into_iter()
        .filter_map(|(labels, mut buckets)| {
            buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

            if buckets.is_empty() {
                return None;
            }

            let total = buckets
                .last()
                .map(|(_, v)| *v)
                .unwrap_or(0.0);
            if total == 0.0 {
                return None;
            }

            let target = phi * total;

            // Find the bucket where cumulative count exceeds target
            let mut prev_le = 0.0_f64;
            let mut prev_count = 0.0_f64;
            let mut result = f64::NAN;

            for &(le, count) in &buckets {
                if count >= target {
                    // Linear interpolation within this bucket
                    let bucket_count = count - prev_count;
                    if bucket_count > 0.0 {
                        let fraction = (target - prev_count) / bucket_count;
                        result = prev_le + (le - prev_le) * fraction;
                    } else {
                        result = prev_le;
                    }
                    break;
                }
                prev_le = le;
                prev_count = count;
            }

            let eval_time = series
                .first()
                .and_then(|s| s.samples.last().map(|(t, _)| *t))
                .unwrap_or(0.0);
            Some(TimeSeries {
                labels,
                samples: vec![(eval_time, result)],
            })
        })
        .collect()
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
        assert_approx(apply_scalar_op(ScalarFunc::Round, 3.456, &[0.1]), 3.5, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Round, 3.456, &[0.01]), 3.46, 0.001);
    }

    #[test]
    fn test_scalar_sqrt() {
        assert_approx(apply_scalar_op(ScalarFunc::Sqrt, 16.0, &[]), 4.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Sqrt, 2.0, &[]), std::f64::consts::SQRT_2, 0.001);
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
        assert_approx(apply_scalar_op(ScalarFunc::ClampMin, -5.0, &[0.0]), 0.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::ClampMin, 5.0, &[0.0]), 5.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::ClampMax, 150.0, &[100.0]), 100.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::ClampMax, 50.0, &[100.0]), 50.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Clamp, -5.0, &[0.0, 100.0]), 0.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Clamp, 50.0, &[0.0, 100.0]), 50.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Clamp, 150.0, &[0.0, 100.0]), 100.0, 0.001);
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
        assert_approx(apply_scalar_op(ScalarFunc::Deg, std::f64::consts::PI, &[]), 180.0, 0.001);
        assert_approx(apply_scalar_op(ScalarFunc::Rad, 180.0, &[]), std::f64::consts::PI, 0.001);
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
}
