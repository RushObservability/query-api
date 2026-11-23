use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Bounds the size of the timestamp predicate in each rolling budget query.
pub const MAX_SLO_MAINTENANCE_WINDOWS: usize = 16;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceRepeat {
    #[default]
    Once,
    Daily,
    Weekly,
}

impl MaintenanceRepeat {
    fn period_seconds(self) -> Option<i64> {
        match self {
            Self::Once => None,
            Self::Daily => Some(86_400),
            Self::Weekly => Some(604_800),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SloMaintenanceWindow {
    pub name: String,
    pub starts_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
    #[serde(default)]
    pub repeat: MaintenanceRepeat,
}

pub fn validate_windows(windows: &mut [SloMaintenanceWindow]) -> Result<(), String> {
    if windows.len() > MAX_SLO_MAINTENANCE_WINDOWS {
        return Err(format!(
            "An SLO can have up to {MAX_SLO_MAINTENANCE_WINDOWS} maintenance windows."
        ));
    }
    for window in windows.iter_mut() {
        window.name = window.name.trim().to_string();
        if window.name.is_empty() || window.name.len() > 255 {
            return Err("Give each maintenance window a name of up to 255 bytes.".into());
        }
        // SLO window bounds and recurrence are evaluated at UTC second precision.
        window.starts_at = DateTime::from_timestamp(window.starts_at.timestamp(), 0)
            .ok_or("Invalid maintenance start time.")?;
        window.ends_at = DateTime::from_timestamp(window.ends_at.timestamp(), 0)
            .ok_or("Invalid maintenance end time.")?;
        let duration = (window.ends_at - window.starts_at).num_seconds();
        let limit = window
            .repeat
            .period_seconds()
            .unwrap_or(crate::models::maintenance::MAX_WINDOW_DAYS * 86_400);
        if duration <= 0 {
            return Err("A maintenance window must end after it starts.".into());
        }
        if duration > limit {
            return Err(
                "A window can last up to 90 days, or one repeat period if recurring.".into(),
            );
        }
    }
    // A deterministic stored order also makes retried saves idempotent.
    windows.sort_by(|a, b| {
        a.starts_at
            .cmp(&b.starts_at)
            .then_with(|| a.name.cmp(&b.name))
    });
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceRange {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SloMaintenanceEvaluation {
    pub revision: u64,
    pub excluded_seconds: i64,
    pub excluded_total: f64,
    pub excluded_bad: f64,
    pub periods: Vec<AppliedMaintenancePeriod>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMaintenancePeriod {
    pub starts_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
}

/// Produce the union of half-open intervals overlapping a rolling evaluation.
/// Leave the final end unclipped for the predicate: the evaluator's upper SQL
/// range is inclusive, so an event exactly at evaluation time may be excluded.
pub fn exclusion_ranges(
    windows: &[SloMaintenanceWindow],
    from: i64,
    to: i64,
) -> Vec<MaintenanceRange> {
    let mut overlaps = Vec::new();
    for window in windows {
        let anchor = window.starts_at.timestamp();
        let duration = window.ends_at.timestamp() - anchor;
        if duration <= 0 {
            continue;
        }
        let period = window.repeat.period_seconds();
        let mut start = period.map_or(anchor, |seconds| {
            anchor + (from - anchor).max(0) / seconds * seconds
        });
        while start <= to {
            let end = start + duration;
            if end > from {
                overlaps.push(MaintenanceRange {
                    start: start.max(from),
                    end,
                });
            }
            let Some(seconds) = period else { break };
            start += seconds;
        }
    }
    overlaps.sort_unstable_by_key(|range| range.start);
    let mut merged: Vec<MaintenanceRange> = Vec::new();
    for range in overlaps {
        if let Some(last) = merged.last_mut()
            && range.start <= last.end
        {
            last.end = last.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    merged
}

impl SloMaintenanceEvaluation {
    pub fn new(
        revision: u64,
        ranges: &[MaintenanceRange],
        to: i64,
        excluded_total: f64,
        excluded_bad: f64,
    ) -> Self {
        let periods = ranges
            .iter()
            .filter_map(|range| {
                Some(AppliedMaintenancePeriod {
                    starts_at: DateTime::from_timestamp(range.start, 0)?,
                    ends_at: DateTime::from_timestamp(range.end.min(to), 0)?,
                })
            })
            .collect();
        Self {
            revision,
            excluded_seconds: ranges
                .iter()
                .map(|range| range.end.min(to) - range.start)
                .sum(),
            excluded_total,
            excluded_bad,
            periods,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(start: i64, end: i64, repeat: MaintenanceRepeat) -> SloMaintenanceWindow {
        SloMaintenanceWindow {
            name: "Planned maintenance".into(),
            starts_at: DateTime::from_timestamp(start, 0).unwrap(),
            ends_at: DateTime::from_timestamp(end, 0).unwrap(),
            repeat,
        }
    }

    #[test]
    fn merges_overlap_and_counts_only_elapsed_coverage() {
        let ranges = exclusion_ranges(
            &[
                window(15, 20, MaintenanceRepeat::Once),
                window(5, 17, MaintenanceRepeat::Once),
                window(21, 35, MaintenanceRepeat::Once),
                window(36, 44, MaintenanceRepeat::Once),
            ],
            10,
            30,
        );
        assert_eq!(
            ranges,
            [
                MaintenanceRange { start: 10, end: 20 },
                MaintenanceRange { start: 21, end: 35 },
            ]
        );
        let used = SloMaintenanceEvaluation::new(2, &ranges, 30, 9.0, 2.0);
        assert_eq!(used.excluded_seconds, 19);
        assert_eq!(used.periods[1].ends_at.timestamp(), 30);
    }

    #[test]
    fn recurrence_keeps_its_utc_anchor_and_skips_old_occurrences() {
        let daily = window(10, 30, MaintenanceRepeat::Daily);
        let weekly = window(20, 35, MaintenanceRepeat::Weekly);
        let week = 604_800 * 100;
        let ranges = exclusion_ranges(&[daily, weekly], week + 25, week + 86_420);
        assert_eq!(
            ranges,
            [
                MaintenanceRange {
                    start: week + 25,
                    end: week + 35
                },
                MaintenanceRange {
                    start: week + 86_410,
                    end: week + 86_430
                },
            ]
        );
    }

    #[test]
    fn validates_zero_duration_and_repeats_longer_than_their_period() {
        assert!(validate_windows(&mut [window(1, 1, MaintenanceRepeat::Once)]).is_err());
        assert!(validate_windows(&mut [window(1, 86_402, MaintenanceRepeat::Daily)]).is_err());
        assert!(validate_windows(&mut [window(1, 86_401, MaintenanceRepeat::Daily)]).is_ok());
    }
}
