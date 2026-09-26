//! Maintenance windows and per-group monitor notification state.
//!
//! A maintenance window silences monitor notifications in one tenant for a
//! period. Monitors keep evaluating and recording state changes; when the
//! window ends, any group whose state differs from its last notification is
//! notified once.

use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

/// Longest window accepted, so a typo can't silence alerts for years.
pub const MAX_WINDOW_DAYS: i64 = 90;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, clickhouse::Row)]
pub struct MaintenanceWindow {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    /// `all`, `monitor:<id>`, or `tag:<key>:<value>`.
    pub scope: String,
    /// RFC 3339 UTC, normalized by [`normalize_time`].
    pub starts_at: String,
    pub ends_at: String,
    pub created_at: String,
    pub created_by: String,
}

/// What a window applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    All,
    Monitor(String),
    Tag { key: String, value: String },
}

impl Scope {
    /// Parse a stored or requested scope. `alert:<id>`, from the retired alert
    /// engine, is read as a monitor id.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        if raw.is_empty() || raw == "all" {
            return Ok(Scope::All);
        }
        if let Some(id) = raw
            .strip_prefix("monitor:")
            .or_else(|| raw.strip_prefix("alert:"))
        {
            let id = id.trim();
            if id.is_empty() {
                return Err("monitor scope needs a monitor id".into());
            }
            return Ok(Scope::Monitor(id.to_string()));
        }
        if let Some(tag) = raw.strip_prefix("tag:") {
            let (key, value) = tag
                .split_once(':')
                .or_else(|| tag.split_once('='))
                .ok_or_else(|| "tag scope must look like tag:key:value".to_string())?;
            let (key, value) = (key.trim(), value.trim());
            if key.is_empty() || value.is_empty() {
                return Err("tag scope must look like tag:key:value".into());
            }
            return Ok(Scope::Tag {
                key: key.to_string(),
                value: value.to_string(),
            });
        }
        Err("scope must be all, monitor:<id>, or tag:<key>:<value>".into())
    }

    /// Canonical stored form.
    pub fn as_stored(&self) -> String {
        match self {
            Scope::All => "all".into(),
            Scope::Monitor(id) => format!("monitor:{id}"),
            Scope::Tag { key, value } => format!("tag:{key}:{value}"),
        }
    }

    /// Whether this scope covers a monitor with these tags (`key:value` pairs).
    pub fn covers(&self, monitor_id: &str, tags: &BTreeMap<String, String>) -> bool {
        match self {
            Scope::All => true,
            Scope::Monitor(id) => id == monitor_id,
            Scope::Tag { key, value } => tags.get(key) == Some(value),
        }
    }
}

/// Parse any RFC 3339 timestamp and store it as UTC with second precision,
/// so stored times compare correctly as text.
pub fn normalize_time(raw: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(raw.trim())
        .map(|t| t.with_timezone(&Utc))
        .map_err(|_| format!("{raw:?} is not an RFC 3339 timestamp, such as 2026-10-01T02:00:00Z"))
}

pub fn format_time(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Check a requested window. Returns normalized (start, end).
pub fn validate_window(
    starts_at: &str,
    ends_at: &str,
    now: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>), String> {
    let start = normalize_time(starts_at)?;
    let end = normalize_time(ends_at)?;
    if end <= start {
        return Err("ends_at must be after starts_at".into());
    }
    if end <= now {
        return Err("ends_at is in the past".into());
    }
    if end - start > chrono::Duration::days(MAX_WINDOW_DAYS) {
        return Err(format!("a window can last at most {MAX_WINDOW_DAYS} days"));
    }
    Ok((start, end))
}

impl MaintenanceWindow {
    /// `scheduled`, `active`, or `ended` at `now`. Unparseable rows count as ended.
    pub fn status(&self, now: DateTime<Utc>) -> &'static str {
        match (
            normalize_time(&self.starts_at),
            normalize_time(&self.ends_at),
        ) {
            (Ok(start), Ok(_)) if now < start => "scheduled",
            (Ok(_), Ok(end)) if now < end => "active",
            _ => "ended",
        }
    }

    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.status(now) == "active"
    }
}

/// Last notification sent for one monitor group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, clickhouse::Row)]
pub struct MonitorNotification {
    pub tenant_id: String,
    pub monitor_id: String,
    pub group_key: String,
    pub state: String,
    pub notified_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        normalize_time(s).unwrap()
    }

    #[test]
    fn parses_scopes() {
        assert_eq!(Scope::parse("all").unwrap(), Scope::All);
        assert_eq!(Scope::parse("").unwrap(), Scope::All);
        assert_eq!(
            Scope::parse("monitor:abc").unwrap(),
            Scope::Monitor("abc".into())
        );
        assert_eq!(
            Scope::parse("alert:abc").unwrap(),
            Scope::Monitor("abc".into())
        );
        assert_eq!(
            Scope::parse("tag:service:checkout").unwrap(),
            Scope::Tag {
                key: "service".into(),
                value: "checkout".into()
            }
        );
        assert_eq!(
            Scope::parse("tag:env=prod").unwrap(),
            Scope::Tag {
                key: "env".into(),
                value: "prod".into()
            }
        );
        assert!(Scope::parse("monitor:").is_err());
        assert!(Scope::parse("tag:service").is_err());
        assert!(Scope::parse("everything").is_err());
        assert_eq!(
            Scope::parse(" tag:env:prod ").unwrap().as_stored(),
            "tag:env:prod"
        );
    }

    #[test]
    fn scope_coverage() {
        let tags = BTreeMap::from([("service".to_string(), "checkout".to_string())]);
        assert!(Scope::All.covers("m1", &tags));
        assert!(Scope::Monitor("m1".into()).covers("m1", &tags));
        assert!(!Scope::Monitor("m2".into()).covers("m1", &tags));
        assert!(
            Scope::parse("tag:service:checkout")
                .unwrap()
                .covers("m1", &tags)
        );
        assert!(
            !Scope::parse("tag:service:cart")
                .unwrap()
                .covers("m1", &tags)
        );
    }

    #[test]
    fn normalizes_times_to_utc_text() {
        assert_eq!(
            format_time(at("2026-10-01T04:00:00+02:00")),
            "2026-10-01T02:00:00Z"
        );
        assert_eq!(
            format_time(at("2026-10-01T02:00:00.750Z")),
            "2026-10-01T02:00:00Z"
        );
        assert!(normalize_time("2026-10-01 02:00").is_err());
    }

    #[test]
    fn validates_windows() {
        let now = at("2026-09-27T00:00:00Z");
        assert!(validate_window("2026-09-27T01:00:00Z", "2026-09-27T03:00:00Z", now).is_ok());
        assert!(
            validate_window("2026-09-26T23:00:00Z", "2026-09-27T01:00:00Z", now).is_ok(),
            "may start in the past"
        );
        assert!(validate_window("2026-09-27T03:00:00Z", "2026-09-27T01:00:00Z", now).is_err());
        assert!(validate_window("2026-09-25T00:00:00Z", "2026-09-26T00:00:00Z", now).is_err());
        assert!(validate_window("2026-09-27T01:00:00Z", "2027-09-27T01:00:00Z", now).is_err());
    }

    #[test]
    fn reports_status() {
        let window = MaintenanceWindow {
            id: "w".into(),
            tenant_id: "default".into(),
            name: "deploy".into(),
            scope: "all".into(),
            starts_at: "2026-09-27T01:00:00Z".into(),
            ends_at: "2026-09-27T02:00:00Z".into(),
            created_at: String::new(),
            created_by: String::new(),
        };
        assert_eq!(window.status(at("2026-09-27T00:59:59Z")), "scheduled");
        assert_eq!(window.status(at("2026-09-27T01:00:00Z")), "active");
        assert_eq!(window.status(at("2026-09-27T02:00:00Z")), "ended");
    }
}
