use time::error::{Parse, ParseFromDescription};
use time::format_description::{BorrowedFormatItem, well_known::Rfc3339};
use time::macros::format_description;
use time::{OffsetDateTime, PrimitiveDateTime};

pub(crate) const DATE: &[BorrowedFormatItem<'static>] = format_description!("[year]-[month]-[day]");
pub(crate) const CLICKHOUSE_SECONDS: &[BorrowedFormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
pub(crate) const CLICKHOUSE_NANOS: &[BorrowedFormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:9]");
pub(crate) const UTC_SECONDS: &[BorrowedFormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z");
pub(crate) const UTC_MILLIS: &[BorrowedFormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z");
pub(crate) const UTC_MICROS: &[BorrowedFormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]Z");
pub(crate) const UTC_NANOS: &[BorrowedFormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z");

pub(crate) fn parse_rfc3339(value: &str) -> Result<OffsetDateTime, Parse> {
    if !matches!(value.as_bytes().get(10), Some(b'T' | b't' | b' ')) {
        return Err(ParseFromDescription::InvalidComponent("date and time separator").into());
    }
    OffsetDateTime::parse(value, &Rfc3339)
}

pub(crate) fn parse_clickhouse(value: &str) -> Result<OffsetDateTime, Parse> {
    PrimitiveDateTime::parse(value, CLICKHOUSE_SECONDS).map(PrimitiveDateTime::assume_utc)
}

pub(crate) fn from_unix_millis(millis: i64) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000).ok()
}

pub(crate) trait TimestampExt {
    fn unix_millis(self) -> i64;
    fn unix_nanos(self) -> Option<i64>;
    fn clickhouse_seconds(self) -> String;
    fn clickhouse_nanos(self) -> String;
    fn utc_seconds(self) -> String;
    fn to_rfc3339(self) -> String;
}

impl TimestampExt for OffsetDateTime {
    fn unix_millis(self) -> i64 {
        (self.unix_timestamp_nanos().div_euclid(1_000_000)) as i64
    }

    fn unix_nanos(self) -> Option<i64> {
        self.unix_timestamp_nanos().try_into().ok()
    }

    fn clickhouse_seconds(self) -> String {
        self.to_utc()
            .format(CLICKHOUSE_SECONDS)
            .expect("valid ClickHouse format")
    }

    fn clickhouse_nanos(self) -> String {
        self.to_utc()
            .format(CLICKHOUSE_NANOS)
            .expect("valid ClickHouse format")
    }

    fn utc_seconds(self) -> String {
        self.to_utc().format(UTC_SECONDS).expect("valid UTC format")
    }

    fn to_rfc3339(self) -> String {
        let mut value = self.format(&Rfc3339).expect("valid RFC 3339 timestamp");
        if value.ends_with('Z') {
            value.pop();
            value.push_str("+00:00");
        }
        let offset_start = value.len() - 6;
        // Keep the existing 3/6/9-digit fractional precision in API output.
        if let Some(fraction_start) = value.find('.') {
            let digits = offset_start - fraction_start - 1;
            for _ in digits..digits.div_ceil(3) * 3 {
                value.insert(offset_start, '0');
            }
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_formats_preserve_offset_precision_and_utc_storage() {
        let ts = parse_rfc3339("2026-11-23T12:00:00.12+05:30").unwrap();
        assert_eq!(ts.to_rfc3339(), "2026-11-23T12:00:00.120+05:30");
        assert_eq!(ts.clickhouse_seconds(), "2026-11-23 06:30:00");
        assert_eq!(ts.clickhouse_nanos(), "2026-11-23 06:30:00.120000000");
        assert_eq!(ts.utc_seconds(), "2026-11-23T06:30:00Z");
        let stored = parse_clickhouse("2026-11-23 06:30:00").unwrap();
        assert_eq!(stored.to_rfc3339(), "2026-11-23T06:30:00+00:00");
    }

    #[test]
    fn timestamps_floor_milliseconds_before_the_epoch() {
        let ts = parse_rfc3339("1969-12-31T23:59:59.999999999Z").unwrap();
        assert_eq!(ts.unix_millis(), -1);
        assert_eq!(ts.unix_nanos(), Some(-1));
        assert_eq!(from_unix_millis(-1).unwrap().unix_nanos(), Some(-1_000_000));
    }

    #[test]
    fn rfc3339_keeps_existing_separator_rules() {
        assert!(parse_rfc3339("2026-11-23 12:00:00Z").is_ok());
        assert!(parse_rfc3339("2026-11-23t12:00:00z").is_ok());
        assert!(parse_rfc3339("2026-11-23X12:00:00Z").is_err());
    }
}
