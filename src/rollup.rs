//! Source-selection for metric reads: pick raw vs 1-minute vs 1-hour pre-aggregated
//! tables automatically based on the query's time span and step.
//!
//! The rollups (`metrics_gauge_1m/1h`, `metrics_sum_1m/1h`) are `AggregatingMergeTree`
//! tables of *State columns built by the materialized views in `migrations.rs`. They
//! pre-aggregate avg/min/max/last/count per `toStartOfInterval` wall-clock bucket. A
//! read against a rollup is only NUMERICALLY IDENTICAL to a raw read when the consumer's
//! own bucketing is itself left-aligned `toStartOfInterval` at (a multiple of) the
//! rollup interval — i.e. the rollup bucket boundaries line up with what the consumer
//! would compute from raw.
//!
//! That holds for:
//!   - whole-window aggregates that don't sub-bucket (e.g. stats ingest `count()` over a
//!     range): `countMerge` over the rollup buckets in the window == raw `count()`.
//!   - left-aligned interval bucketing whose interval is a multiple of the rollup
//!     interval and whose grid is wall-clock aligned.
//!
//! It does NOT hold for PromQL's instant-vector step alignment, which uses *centered*
//! windows `[t-step/2, t+step/2]` on an arbitrary (not necessarily wall-clock) step grid.
//! Those reads stay on raw (see `promql::eval`), where we push the centered-window
//! bucketing into SQL but read raw samples.

/// Span (seconds) at/under which a read always uses raw, regardless of step. Recent,
/// short windows are cheap on raw and avoid any rollup-freshness edge (the live MV may
/// be a few seconds behind raw for the in-progress bucket).
pub const RAW_MAX_SPAN_SECS: f64 = 6.0 * 3600.0; // 6 hours

/// Span (seconds) above `RAW_MAX_SPAN_SECS` and at/under this uses the 1-minute rollup.
/// Above this, the 1-hour rollup is used.
pub const ONE_MIN_MAX_SPAN_SECS: f64 = 2.0 * 24.0 * 3600.0; // 2 days

/// Rollup interval seconds.
pub const MIN_INTERVAL_SECS: i64 = 60;
pub const HOUR_INTERVAL_SECS: i64 = 3600;

/// Which physical source a metric read should target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// Raw `metrics_gauge` / `metrics_sum`.
    Raw,
    /// 1-minute rollup `metrics_gauge_1m` / `metrics_sum_1m`.
    Rollup1m,
    /// 1-hour rollup `metrics_gauge_1h` / `metrics_sum_1h`.
    Rollup1h,
}

impl Source {
    /// Table-name suffix for this source ("", "_1m", "_1h").
    pub fn suffix(self) -> &'static str {
        match self {
            Source::Raw => "",
            Source::Rollup1m => "_1m",
            Source::Rollup1h => "_1h",
        }
    }

    /// The wall-clock bucket interval (seconds) this source pre-aggregates at, or `None`
    /// for raw.
    pub fn interval_secs(self) -> Option<i64> {
        match self {
            Source::Raw => None,
            Source::Rollup1m => Some(MIN_INTERVAL_SECS),
            Source::Rollup1h => Some(HOUR_INTERVAL_SECS),
        }
    }
}

/// Choose a source for a *whole-window* aggregation (no sub-bucketing), e.g. a
/// `count()`/`sum(count)` over `[start, end]`. The result aggregates whole rollup
/// buckets, so any rollup whose interval is smaller than the span is exact; we pick the
/// coarsest rollup that still fully tiles the window cleanly.
///
/// Rule (named constants above):
///   - span <= RAW_MAX_SPAN_SECS                       → Raw
///   - RAW_MAX_SPAN_SECS < span <= ONE_MIN_MAX_SPAN_SECS → Rollup1m
///   - span > ONE_MIN_MAX_SPAN_SECS                     → Rollup1h
pub fn select_window_source(start_secs: f64, end_secs: f64) -> Source {
    let span = (end_secs - start_secs).max(0.0);
    if span <= RAW_MAX_SPAN_SECS {
        Source::Raw
    } else if span <= ONE_MIN_MAX_SPAN_SECS {
        Source::Rollup1m
    } else {
        Source::Rollup1h
    }
}

/// Choose a source for a *left-aligned interval bucketing* read: the consumer buckets by
/// `toStartOfInterval(ts, INTERVAL bucket_secs)`. A rollup is exact only when its
/// interval evenly divides `bucket_secs` (so each consumer bucket is an exact union of
/// whole rollup buckets) AND the span is large enough to bother. Otherwise raw.
pub fn select_interval_source(start_secs: f64, end_secs: f64, bucket_secs: i64) -> Source {
    if bucket_secs <= 0 {
        return Source::Raw;
    }
    let coarse = select_window_source(start_secs, end_secs);
    match coarse {
        Source::Rollup1h if bucket_secs % HOUR_INTERVAL_SECS == 0 => Source::Rollup1h,
        // Fall back to 1m if the bucket is a clean multiple of a minute (covers the
        // 1h-span-but-sub-hour-bucket case too).
        Source::Rollup1h | Source::Rollup1m if bucket_secs % MIN_INTERVAL_SECS == 0 => {
            Source::Rollup1m
        }
        _ => Source::Raw,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_source_thresholds() {
        let t0 = 1_000_000.0;
        // 1 hour → raw
        assert_eq!(select_window_source(t0, t0 + 3600.0), Source::Raw);
        // exactly 6h → raw (boundary inclusive)
        assert_eq!(select_window_source(t0, t0 + RAW_MAX_SPAN_SECS), Source::Raw);
        // 12h → 1m
        assert_eq!(select_window_source(t0, t0 + 12.0 * 3600.0), Source::Rollup1m);
        // exactly 2 days → 1m (boundary inclusive)
        assert_eq!(
            select_window_source(t0, t0 + ONE_MIN_MAX_SPAN_SECS),
            Source::Rollup1m
        );
        // 7 days → 1h
        assert_eq!(
            select_window_source(t0, t0 + 7.0 * 24.0 * 3600.0),
            Source::Rollup1h
        );
        // 30 days → 1h
        assert_eq!(
            select_window_source(t0, t0 + 30.0 * 24.0 * 3600.0),
            Source::Rollup1h
        );
    }

    #[test]
    fn interval_source_requires_divisible_bucket() {
        let t0 = 1_000_000.0;
        // 30-day span, 1-hour bucket → 1h rollup (divides cleanly)
        assert_eq!(
            select_interval_source(t0, t0 + 30.0 * 86400.0, 3600),
            Source::Rollup1h
        );
        // 30-day span, 120-second bucket → multiple of a minute (not an hour) → 1m rollup
        assert_eq!(
            select_interval_source(t0, t0 + 30.0 * 86400.0, 120),
            Source::Rollup1m
        );
        // 30-day span, 90-second bucket → not a clean minute multiple → raw
        assert_eq!(
            select_interval_source(t0, t0 + 30.0 * 86400.0, 90),
            Source::Raw
        );
        // 30-day span, 30-second bucket → not a clean minute multiple → raw
        assert_eq!(
            select_interval_source(t0, t0 + 30.0 * 86400.0, 30),
            Source::Raw
        );
        // short span always raw
        assert_eq!(select_interval_source(t0, t0 + 600.0, 3600), Source::Raw);
    }

    #[test]
    fn suffix_and_interval() {
        assert_eq!(Source::Raw.suffix(), "");
        assert_eq!(Source::Rollup1m.suffix(), "_1m");
        assert_eq!(Source::Rollup1h.suffix(), "_1h");
        assert_eq!(Source::Raw.interval_secs(), None);
        assert_eq!(Source::Rollup1m.interval_secs(), Some(60));
        assert_eq!(Source::Rollup1h.interval_secs(), Some(3600));
    }
}
