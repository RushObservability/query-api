use super::types::RangeFunc;

// ═══════════════════════════════════════════════════════════════════
// Range function implementations
// ═══════════════════════════════════════════════════════════════════

/// Dispatch a range function computation over a window of samples.
pub fn evaluate_range_func(
    func: RangeFunc,
    samples: &[(f64, f64)],
    param: Option<f64>,
) -> Option<f64> {
    match func {
        RangeFunc::Rate => compute_rate(samples),
        RangeFunc::Irate => compute_irate(samples),
        RangeFunc::Increase => compute_increase(samples),
        RangeFunc::SumOverTime => compute_sum_over_time(samples),
        RangeFunc::AvgOverTime => compute_avg_over_time(samples),
        RangeFunc::MinOverTime => compute_min_over_time(samples),
        RangeFunc::MaxOverTime => compute_max_over_time(samples),
        RangeFunc::CountOverTime => compute_count_over_time(samples),
        RangeFunc::StddevOverTime => compute_stddev_over_time(samples),
        RangeFunc::StdvarOverTime => compute_stdvar_over_time(samples),
        RangeFunc::QuantileOverTime => {
            compute_quantile_over_time(param.unwrap_or(0.5), samples)
        }
        RangeFunc::LastOverTime => compute_last_over_time(samples),
        RangeFunc::FirstOverTime => compute_first_over_time(samples),
        RangeFunc::Delta => compute_delta(samples),
        RangeFunc::Idelta => compute_idelta(samples),
        RangeFunc::Deriv => compute_deriv(samples),
        RangeFunc::PredictLinear => {
            compute_predict_linear(samples, param.unwrap_or(0.0))
        }
        RangeFunc::Changes => compute_changes(samples),
        RangeFunc::Resets => compute_resets(samples),
        RangeFunc::AbsentOverTime => compute_absent_over_time(samples),
        RangeFunc::PresentOverTime => compute_present_over_time(samples),
    }
}

/// Compute rate: total_increase / dt. Handles counter resets.
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

/// Sum of all values in the range.
pub fn compute_sum_over_time(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    Some(samples.iter().map(|(_, v)| v).sum())
}

/// Average of all values in the range.
pub fn compute_avg_over_time(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let sum: f64 = samples.iter().map(|(_, v)| v).sum();
    Some(sum / samples.len() as f64)
}

/// Minimum value in the range.
pub fn compute_min_over_time(samples: &[(f64, f64)]) -> Option<f64> {
    samples.iter().map(|(_, v)| *v).reduce(f64::min)
}

/// Maximum value in the range.
pub fn compute_max_over_time(samples: &[(f64, f64)]) -> Option<f64> {
    samples.iter().map(|(_, v)| *v).reduce(f64::max)
}

/// Count of samples in the range.
pub fn compute_count_over_time(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    Some(samples.len() as f64)
}

/// Population standard deviation of values in the range.
pub fn compute_stddev_over_time(samples: &[(f64, f64)]) -> Option<f64> {
    compute_stdvar_over_time(samples).map(|v| v.sqrt())
}

/// Population variance of values in the range.
pub fn compute_stdvar_over_time(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let n = samples.len() as f64;
    let mean = samples.iter().map(|(_, v)| v).sum::<f64>() / n;
    let variance = samples.iter().map(|(_, v)| (v - mean).powi(2)).sum::<f64>() / n;
    Some(variance)
}

/// Quantile over time using linear interpolation.
pub fn compute_quantile_over_time(q: f64, samples: &[(f64, f64)]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut values: Vec<f64> = samples.iter().map(|(_, v)| *v).collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Some(quantile_sorted(&values, q))
}

/// Compute quantile from a sorted slice of values using linear interpolation.
pub fn quantile_sorted(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let q = q.clamp(0.0, 1.0);
    let rank = q * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper || lower + 1 >= sorted.len() {
        return sorted[lower.min(sorted.len() - 1)];
    }
    let fraction = rank - lower as f64;
    sorted[lower] + (sorted[upper] - sorted[lower]) * fraction
}

/// Last value in the range.
pub fn compute_last_over_time(samples: &[(f64, f64)]) -> Option<f64> {
    samples.last().map(|(_, v)| *v)
}

/// First value in the range.
pub fn compute_first_over_time(samples: &[(f64, f64)]) -> Option<f64> {
    samples.first().map(|(_, v)| *v)
}

/// Delta: difference between last and first value (for gauges).
pub fn compute_delta(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let first = samples.first().unwrap().1;
    let last = samples.last().unwrap().1;
    Some(last - first)
}

/// Idelta: difference between the last two samples.
pub fn compute_idelta(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let prev = samples[samples.len() - 2].1;
    let last = samples[samples.len() - 1].1;
    Some(last - prev)
}

/// Deriv: slope of linear regression over the samples.
pub fn compute_deriv(samples: &[(f64, f64)]) -> Option<f64> {
    linear_regression_slope(samples)
}

/// Predict linear: predict value at t seconds after the last sample using linear regression.
pub fn compute_predict_linear(samples: &[(f64, f64)], t_secs: f64) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let slope = linear_regression_slope(samples)?;
    let n = samples.len() as f64;
    let mean_x = samples.iter().map(|(t, _)| t).sum::<f64>() / n;
    let mean_y = samples.iter().map(|(_, v)| v).sum::<f64>() / n;
    let intercept = mean_y - slope * mean_x;
    let last_t = samples.last().unwrap().0;
    Some(slope * (last_t + t_secs) + intercept)
}

/// Compute the slope of a least-squares linear regression.
pub fn linear_regression_slope(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let n = samples.len() as f64;
    let sum_x: f64 = samples.iter().map(|(t, _)| t).sum();
    let sum_y: f64 = samples.iter().map(|(_, v)| v).sum();
    let sum_xy: f64 = samples.iter().map(|(t, v)| t * v).sum();
    let sum_x2: f64 = samples.iter().map(|(t, _)| t * t).sum();

    let denom = n * sum_x2 - sum_x * sum_x;
    if denom.abs() < f64::EPSILON {
        return None;
    }
    Some((n * sum_xy - sum_x * sum_y) / denom)
}

/// Changes: count how many times the value changed.
pub fn compute_changes(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let count = samples
        .windows(2)
        .filter(|w| (w[1].1 - w[0].1).abs() > f64::EPSILON)
        .count();
    Some(count as f64)
}

/// Resets: count how many times the value decreased (counter resets).
pub fn compute_resets(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let count = samples.windows(2).filter(|w| w[1].1 < w[0].1).count();
    Some(count as f64)
}

/// Absent over time: returns 1 if there are no samples, None otherwise.
pub fn compute_absent_over_time(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.is_empty() {
        Some(1.0)
    } else {
        None
    }
}

/// Present over time: returns 1 if there are samples, None otherwise.
pub fn compute_present_over_time(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.is_empty() {
        None
    } else {
        Some(1.0)
    }
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── Test data from VictoriaMetrics ──
    // Source: app/vmselect/promql/rollup_test.go

    fn test_samples() -> Vec<(f64, f64)> {
        let timestamps = [5.0, 15.0, 24.0, 36.0, 49.0, 60.0, 78.0, 80.0, 97.0, 115.0, 120.0, 130.0];
        let values = [123.0, 34.0, 44.0, 21.0, 54.0, 34.0, 99.0, 12.0, 44.0, 32.0, 34.0, 34.0];
        timestamps.iter().zip(values.iter()).map(|(t, v)| (*t, *v)).collect()
    }

    fn assert_approx(actual: f64, expected: f64, epsilon: f64) {
        assert!(
            (actual - expected).abs() < epsilon,
            "expected {expected}, got {actual} (diff: {})",
            (actual - expected).abs()
        );
    }

    #[test]
    fn test_compute_rate() {
        let samples = test_samples();
        let rate = compute_rate(&samples).unwrap();
        assert_approx(rate, 2.2, 0.001);
    }

    #[test]
    fn test_compute_irate() {
        let samples = test_samples();
        let irate = compute_irate(&samples).unwrap();
        assert_approx(irate, 0.0, 0.001);
    }

    #[test]
    fn test_compute_increase() {
        let samples = test_samples();
        let inc = compute_increase(&samples).unwrap();
        assert_approx(inc, 275.0, 0.001);
    }

    #[test]
    fn test_compute_sum_over_time() {
        let samples = test_samples();
        let sum = compute_sum_over_time(&samples).unwrap();
        assert_approx(sum, 565.0, 0.001);
    }

    #[test]
    fn test_compute_avg_over_time() {
        let samples = test_samples();
        let avg = compute_avg_over_time(&samples).unwrap();
        assert_approx(avg, 565.0 / 12.0, 0.001);
    }

    #[test]
    fn test_compute_min_over_time() {
        let samples = test_samples();
        let min = compute_min_over_time(&samples).unwrap();
        assert_approx(min, 12.0, 0.001);
    }

    #[test]
    fn test_compute_max_over_time() {
        let samples = test_samples();
        let max = compute_max_over_time(&samples).unwrap();
        assert_approx(max, 123.0, 0.001);
    }

    #[test]
    fn test_compute_count_over_time() {
        let samples = test_samples();
        let count = compute_count_over_time(&samples).unwrap();
        assert_approx(count, 12.0, 0.001);
    }

    #[test]
    fn test_compute_stdvar_over_time() {
        let samples = test_samples();
        let var = compute_stdvar_over_time(&samples).unwrap();
        assert_approx(var, 945.743, 0.1);
    }

    #[test]
    fn test_compute_stddev_over_time() {
        let samples = test_samples();
        let sd = compute_stddev_over_time(&samples).unwrap();
        assert_approx(sd, 30.753, 0.01);
    }

    #[test]
    fn test_compute_quantile_over_time_median() {
        let samples = test_samples();
        let q50 = compute_quantile_over_time(0.5, &samples).unwrap();
        assert_approx(q50, 34.0, 0.001);
    }

    #[test]
    fn test_compute_quantile_over_time_p0() {
        let samples = test_samples();
        let q0 = compute_quantile_over_time(0.0, &samples).unwrap();
        assert_approx(q0, 12.0, 0.001);
    }

    #[test]
    fn test_compute_quantile_over_time_p100() {
        let samples = test_samples();
        let q100 = compute_quantile_over_time(1.0, &samples).unwrap();
        assert_approx(q100, 123.0, 0.001);
    }

    #[test]
    fn test_compute_quantile_over_time_p90() {
        let samples = test_samples();
        let q90 = compute_quantile_over_time(0.9, &samples).unwrap();
        assert_approx(q90, 94.5, 0.001);
    }

    #[test]
    fn test_compute_last_over_time() {
        let samples = test_samples();
        let last = compute_last_over_time(&samples).unwrap();
        assert_approx(last, 34.0, 0.001);
    }

    #[test]
    fn test_compute_first_over_time() {
        let samples = test_samples();
        let first = compute_first_over_time(&samples).unwrap();
        assert_approx(first, 123.0, 0.001);
    }

    #[test]
    fn test_compute_delta() {
        let samples = test_samples();
        let delta = compute_delta(&samples).unwrap();
        assert_approx(delta, -89.0, 0.001);
    }

    #[test]
    fn test_compute_idelta() {
        let samples = test_samples();
        let idelta = compute_idelta(&samples).unwrap();
        assert_approx(idelta, 0.0, 0.001);
    }

    #[test]
    fn test_compute_changes() {
        let samples = test_samples();
        let ch = compute_changes(&samples).unwrap();
        assert_approx(ch, 10.0, 0.001);
    }

    #[test]
    fn test_compute_resets() {
        let samples = test_samples();
        let r = compute_resets(&samples).unwrap();
        assert_approx(r, 5.0, 0.001);
    }

    #[test]
    fn test_compute_deriv() {
        let samples = test_samples();
        let slope = compute_deriv(&samples).unwrap();
        assert_approx(slope, -0.2669, 0.001);
    }

    #[test]
    fn test_compute_predict_linear() {
        let samples = test_samples();
        let predicted = compute_predict_linear(&samples, 60.0).unwrap();
        assert!(predicted > -100.0 && predicted < 200.0, "predicted = {predicted}");
    }

    #[test]
    fn test_compute_absent_over_time() {
        let samples = test_samples();
        assert_eq!(compute_absent_over_time(&samples), None);
        assert_eq!(compute_absent_over_time(&[]), Some(1.0));
    }

    #[test]
    fn test_compute_present_over_time() {
        let samples = test_samples();
        assert_eq!(compute_present_over_time(&samples), Some(1.0));
        assert_eq!(compute_present_over_time(&[]), None);
    }

    #[test]
    fn test_empty_samples() {
        let empty: Vec<(f64, f64)> = vec![];
        assert_eq!(compute_rate(&empty), None);
        assert_eq!(compute_irate(&empty), None);
        assert_eq!(compute_increase(&empty), None);
        assert_eq!(compute_sum_over_time(&empty), None);
        assert_eq!(compute_avg_over_time(&empty), None);
        assert_eq!(compute_min_over_time(&empty), None);
        assert_eq!(compute_max_over_time(&empty), None);
        assert_eq!(compute_count_over_time(&empty), None);
        assert_eq!(compute_stddev_over_time(&empty), None);
        assert_eq!(compute_stdvar_over_time(&empty), None);
        assert_eq!(compute_delta(&empty), None);
        assert_eq!(compute_idelta(&empty), None);
        assert_eq!(compute_deriv(&empty), None);
        assert_eq!(compute_changes(&empty), None);
        assert_eq!(compute_resets(&empty), None);
    }

    #[test]
    fn test_single_sample() {
        let single = vec![(10.0, 42.0)];
        assert_eq!(compute_rate(&single), None);
        assert_eq!(compute_irate(&single), None);
        assert_eq!(compute_delta(&single), None);
        assert_eq!(compute_idelta(&single), None);
        assert_eq!(compute_deriv(&single), None);
        assert_eq!(compute_sum_over_time(&single), Some(42.0));
        assert_eq!(compute_avg_over_time(&single), Some(42.0));
        assert_eq!(compute_min_over_time(&single), Some(42.0));
        assert_eq!(compute_max_over_time(&single), Some(42.0));
        assert_eq!(compute_count_over_time(&single), Some(1.0));
        assert_eq!(compute_last_over_time(&single), Some(42.0));
        assert_eq!(compute_first_over_time(&single), Some(42.0));
        assert_approx(compute_changes(&single).unwrap(), 0.0, 0.001);
        assert_approx(compute_resets(&single).unwrap(), 0.0, 0.001);
    }

    #[test]
    fn test_two_samples() {
        let two = vec![(10.0, 100.0), (20.0, 200.0)];
        assert_approx(compute_rate(&two).unwrap(), 10.0, 0.001);
        assert_approx(compute_irate(&two).unwrap(), 10.0, 0.001);
        assert_approx(compute_increase(&two).unwrap(), 100.0, 0.001);
        assert_approx(compute_delta(&two).unwrap(), 100.0, 0.001);
        assert_approx(compute_idelta(&two).unwrap(), 100.0, 0.001);
        assert_approx(compute_deriv(&two).unwrap(), 10.0, 0.001);
        assert_approx(compute_sum_over_time(&two).unwrap(), 300.0, 0.001);
        assert_approx(compute_avg_over_time(&two).unwrap(), 150.0, 0.001);
    }

    #[test]
    fn test_counter_reset() {
        let samples = vec![
            (0.0, 0.0),
            (10.0, 5.0),
            (20.0, 10.0),
            (30.0, 3.0),  // reset
            (40.0, 8.0),
        ];
        let rate = compute_rate(&samples).unwrap();
        assert_approx(rate, 18.0 / 40.0, 0.001);
        assert_approx(compute_resets(&samples).unwrap(), 1.0, 0.001);
    }

    #[test]
    fn test_constant_values() {
        let constant = vec![
            (10.0, 42.0),
            (20.0, 42.0),
            (30.0, 42.0),
        ];
        assert_approx(compute_rate(&constant).unwrap(), 0.0, 0.001);
        assert_approx(compute_delta(&constant).unwrap(), 0.0, 0.001);
        assert_approx(compute_stdvar_over_time(&constant).unwrap(), 0.0, 0.001);
        assert_approx(compute_changes(&constant).unwrap(), 0.0, 0.001);
    }

    #[test]
    fn test_evaluate_range_func_dispatch() {
        let samples = test_samples();
        assert_approx(
            evaluate_range_func(RangeFunc::Rate, &samples, None).unwrap(),
            compute_rate(&samples).unwrap(),
            0.001,
        );
        assert_approx(
            evaluate_range_func(RangeFunc::SumOverTime, &samples, None).unwrap(),
            compute_sum_over_time(&samples).unwrap(),
            0.001,
        );
        assert_approx(
            evaluate_range_func(RangeFunc::Delta, &samples, None).unwrap(),
            compute_delta(&samples).unwrap(),
            0.001,
        );
        assert_approx(
            evaluate_range_func(RangeFunc::Changes, &samples, None).unwrap(),
            compute_changes(&samples).unwrap(),
            0.001,
        );
    }

    #[test]
    fn test_quantile_sorted() {
        let sorted = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_approx(quantile_sorted(&sorted, 0.0), 1.0, 0.001);
        assert_approx(quantile_sorted(&sorted, 0.5), 3.0, 0.001);
        assert_approx(quantile_sorted(&sorted, 1.0), 5.0, 0.001);
        assert_approx(quantile_sorted(&sorted, 0.25), 2.0, 0.001);
        assert_approx(quantile_sorted(&sorted, 0.75), 4.0, 0.001);
    }

    #[test]
    fn test_linear_regression_perfect() {
        let samples = vec![(1.0, 3.0), (2.0, 5.0), (3.0, 7.0), (4.0, 9.0)];
        let slope = linear_regression_slope(&samples).unwrap();
        assert_approx(slope, 2.0, 0.001);
    }

    #[test]
    fn test_predict_linear_perfect() {
        let samples = vec![(1.0, 3.0), (2.0, 5.0), (3.0, 7.0), (4.0, 9.0)];
        let predicted = compute_predict_linear(&samples, 5.0).unwrap();
        assert_approx(predicted, 19.0, 0.001);
    }
}
