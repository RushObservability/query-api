//! Incident notebooks: an ordered list of notes and query cells about one
//! incident. Each query cell carries its own time setting, resolved by the
//! client against the notebook's incident window.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::models::maintenance::{format_time, normalize_time};

pub const CELL_TYPES: &[&str] = &["note", "logs", "spans", "panel", "promql"];
pub const MAX_CELLS: u64 = 200;
pub const MAX_BODY_BYTES: usize = 64 * 1024;
pub const MAX_CONFIG_BYTES: usize = 16 * 1024;
/// Longest range one cell may query, so a typo can't scan months of data.
pub const MAX_SPAN_DAYS: i64 = 31;
/// Gap between positions, so inserting between two cells rarely renumbers.
pub const POSITION_STEP: u32 = 1000;

#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct Notebook {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub description: String,
    pub owner_id: String,
    pub owner_name: String,
    /// `private` (owner only) or `tenant`.
    pub visibility: String,
    /// `open` or `resolved`.
    pub status: String,
    /// RFC 3339 UTC.
    pub incident_start: String,
    /// RFC 3339 UTC, or empty while the incident is ongoing.
    pub incident_end: String,
    /// JSON array of strings.
    pub tags: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct NotebookCell {
    pub id: String,
    pub notebook_id: String,
    pub tenant_id: String,
    pub position: u32,
    pub cell_type: String,
    pub title: String,
    /// Markdown, for note cells.
    pub body: String,
    /// JSON `WidgetQueryConfig`; `time` replaces its `time_range_minutes`.
    pub query_config: String,
    /// JSON [`CellTime`].
    pub time: String,
    pub display_config: String,
    pub author_id: String,
    pub author_name: String,
    pub created_at: String,
    pub updated_at: String,
}

/// When a query cell looks. The client resolves it to a fixed `from`/`to`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum CellTime {
    /// The notebook's incident window.
    #[default]
    Inherit,
    Absolute {
        from: String,
        to: String,
    },
    /// Minutes from the incident start; negative looks before it.
    Relative {
        offset_from_min: i64,
        offset_to_min: i64,
    },
    /// The last `minutes` up to now.
    Live {
        minutes: u32,
    },
}

impl CellTime {
    /// Check the setting and normalize absolute times to RFC 3339 UTC.
    pub fn validated(self) -> Result<Self, String> {
        let max_minutes = MAX_SPAN_DAYS * 24 * 60;
        match self {
            CellTime::Inherit => Ok(CellTime::Inherit),
            CellTime::Absolute { from, to } => {
                let (from, to) = (normalize_time(&from)?, normalize_time(&to)?);
                check_span(from, to)?;
                Ok(CellTime::Absolute {
                    from: format_time(from),
                    to: format_time(to),
                })
            }
            CellTime::Relative {
                offset_from_min,
                offset_to_min,
            } => {
                if offset_from_min >= offset_to_min {
                    return Err("offset_from_min must be before offset_to_min".into());
                }
                if offset_from_min.abs() > max_minutes || offset_to_min.abs() > max_minutes {
                    return Err(format!("offsets must be within {MAX_SPAN_DAYS} days"));
                }
                Ok(self)
            }
            CellTime::Live { minutes } => {
                if !(1..=1440).contains(&minutes) {
                    return Err("live minutes must be between 1 and 1440".into());
                }
                Ok(self)
            }
        }
    }

    pub fn mode(&self) -> &'static str {
        match self {
            CellTime::Inherit => "inherit",
            CellTime::Absolute { .. } => "absolute",
            CellTime::Relative { .. } => "relative",
            CellTime::Live { .. } => "live",
        }
    }
}

fn check_span(from: DateTime<Utc>, to: DateTime<Utc>) -> Result<(), String> {
    if to <= from {
        return Err("the end must be after the start".into());
    }
    if to - from > Duration::days(MAX_SPAN_DAYS) {
        return Err(format!("a range can't be longer than {MAX_SPAN_DAYS} days"));
    }
    Ok(())
}

/// Edit stamp, precise enough that two saves in one second still differ for
/// the stale-write check.
pub fn stamp(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

/// Check an incident window. Returns normalized (start, end); an empty end
/// means the incident is ongoing.
pub fn validate_incident_window(start: &str, end: &str) -> Result<(String, String), String> {
    let start = normalize_time(start)?;
    if end.trim().is_empty() {
        return Ok((format_time(start), String::new()));
    }
    let end = normalize_time(end)?;
    check_span(start, end)?;
    Ok((format_time(start), format_time(end)))
}

/// A JSON object stored as text, capped so a cell can't hold megabytes.
pub fn config_json(value: Option<serde_json::Value>, field: &str) -> Result<String, String> {
    let value = value.unwrap_or_else(|| serde_json::json!({}));
    if !value.is_object() {
        return Err(format!("{field} must be a JSON object"));
    }
    let text = value.to_string();
    if text.len() > MAX_CONFIG_BYTES {
        return Err(format!("{field} must not exceed {MAX_CONFIG_BYTES} bytes"));
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(json: serde_json::Value) -> Result<CellTime, String> {
        serde_json::from_value::<CellTime>(json)
            .map_err(|e| e.to_string())?
            .validated()
    }

    #[test]
    fn every_mode_parses_and_absolute_times_are_normalized() {
        assert_eq!(
            time(serde_json::json!({"mode": "inherit"})),
            Ok(CellTime::Inherit)
        );
        assert_eq!(
            time(
                serde_json::json!({"mode": "absolute", "from": "2026-11-07T14:02:00+01:00", "to": "2026-11-07T13:20:00Z"})
            ),
            Ok(CellTime::Absolute {
                from: "2026-11-07T13:02:00Z".into(),
                to: "2026-11-07T13:20:00Z".into()
            })
        );
        assert!(
            time(
                serde_json::json!({"mode": "relative", "offset_from_min": -15, "offset_to_min": 30})
            )
            .is_ok()
        );
        assert!(time(serde_json::json!({"mode": "live", "minutes": 15})).is_ok());
    }

    #[test]
    fn bad_ranges_are_rejected() {
        for bad in [
            serde_json::json!({"mode": "absolute", "from": "2026-11-07T14:20:00Z", "to": "2026-11-07T14:02:00Z"}),
            serde_json::json!({"mode": "absolute", "from": "2026-01-01T00:00:00Z", "to": "2026-03-01T00:00:00Z"}),
            serde_json::json!({"mode": "absolute", "from": "yesterday", "to": "2026-11-07T14:02:00Z"}),
            serde_json::json!({"mode": "relative", "offset_from_min": 30, "offset_to_min": -15}),
            serde_json::json!({"mode": "relative", "offset_from_min": -50000, "offset_to_min": 0}),
            serde_json::json!({"mode": "live", "minutes": 0}),
            serde_json::json!({"mode": "live", "minutes": 1441}),
            serde_json::json!({"mode": "sometimes"}),
        ] {
            assert!(time(bad.clone()).is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn incident_window_may_be_ongoing_but_not_backwards() {
        assert_eq!(
            validate_incident_window("2026-11-07T13:40:00Z", ""),
            Ok(("2026-11-07T13:40:00Z".into(), String::new()))
        );
        assert!(validate_incident_window("2026-11-07T13:40:00Z", "2026-11-07T19:10:00Z").is_ok());
        assert!(validate_incident_window("2026-11-07T13:40:00Z", "2026-11-07T13:00:00Z").is_err());
        assert!(validate_incident_window("", "").is_err());
    }

    #[test]
    fn configs_must_be_small_objects() {
        assert_eq!(config_json(None, "query_config"), Ok("{}".into()));
        assert!(config_json(Some(serde_json::json!([1])), "query_config").is_err());
        let big = serde_json::json!({"q": "x".repeat(MAX_CONFIG_BYTES)});
        assert!(config_json(Some(big), "query_config").is_err());
    }
}
