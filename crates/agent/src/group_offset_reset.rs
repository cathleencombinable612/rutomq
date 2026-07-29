use chrono::{DateTime, Utc};
use regex::Regex;
use rutomq_control::ControlError;
use std::sync::OnceLock;

const NANOS_PER_MILLISECOND: i128 = 1_000_000;
const NANOS_PER_SECOND: i128 = 1_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ShareOffsetReset {
    Earliest,
    Latest,
    ByDuration {
        configured: String,
        duration_nanos: i128,
    },
}

impl ShareOffsetReset {
    pub(super) fn configured_value(&self) -> &str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::ByDuration { configured, .. } => configured,
        }
    }

    pub(super) fn target_timestamp_ms(&self, now: DateTime<Utc>) -> Option<i64> {
        let Self::ByDuration { duration_nanos, .. } = self else {
            return None;
        };
        let now_nanos = now
            .timestamp_nanos_opt()
            .map(i128::from)
            .unwrap_or_else(|| i128::from(now.timestamp_millis()) * NANOS_PER_MILLISECOND);
        let timestamp = (now_nanos - duration_nanos).div_euclid(NANOS_PER_MILLISECOND);
        Some(timestamp.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64)
    }
}

pub(super) fn parse_share_offset_reset(value: &str) -> Result<ShareOffsetReset, ControlError> {
    match value {
        "earliest" => Ok(ShareOffsetReset::Earliest),
        "latest" => Ok(ShareOffsetReset::Latest),
        _ => {
            let duration = value
                .strip_prefix("by_duration:")
                .and_then(parse_duration_nanos)
                .ok_or_else(|| invalid_strategy(value))?;
            Ok(ShareOffsetReset::ByDuration {
                configured: value.to_owned(),
                duration_nanos: duration,
            })
        }
    }
}

fn parse_duration_nanos(value: &str) -> Option<i128> {
    if value.ends_with(['T', 't']) {
        return None;
    }
    static DURATION: OnceLock<Regex> = OnceLock::new();
    let expression = DURATION.get_or_init(|| {
        Regex::new(r"(?i)^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)(?:[\.,](\d{1,9}))?S)?)?$")
            .expect("ISO-8601 duration pattern is valid")
    });
    let captures = expression.captures(value)?;
    if (1..=5).all(|index| captures.get(index).is_none()) {
        return None;
    }
    let days = component(&captures, 1)?;
    let hours = component(&captures, 2)?;
    let minutes = component(&captures, 3)?;
    let seconds = component(&captures, 4)?;
    let whole_seconds = days
        .checked_mul(86_400)?
        .checked_add(hours.checked_mul(3_600)?)?
        .checked_add(minutes.checked_mul(60)?)?
        .checked_add(seconds)?;
    let fractional = captures.get(5).map_or(Some(0), |capture| {
        let value = capture.as_str();
        let nanos = value.parse::<i128>().ok()?;
        nanos.checked_mul(10_i128.pow((9 - value.len()) as u32))
    })?;
    whole_seconds
        .checked_mul(NANOS_PER_SECOND)?
        .checked_add(fractional)
}

fn component(captures: &regex::Captures<'_>, index: usize) -> Option<i128> {
    captures
        .get(index)
        .map_or(Some(0), |capture| capture.as_str().parse().ok())
}

fn invalid_strategy(value: &str) -> ControlError {
    ControlError::InvalidRequest(format!(
        "group configuration share.auto.offset.reset has invalid value {value}; expected earliest, latest, or by_duration:PnDTnHnMn.nS"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn parses_duration_reset_and_computes_record_timestamp() {
        let reset = parse_share_offset_reset("by_duration:P1DT2H3M4.005S").unwrap();
        assert_eq!(reset.configured_value(), "by_duration:P1DT2H3M4.005S");
        let now = Utc.timestamp_millis_opt(100_000_000).unwrap();
        assert_eq!(reset.target_timestamp_ms(now), Some(6_215_995));
    }

    #[test]
    fn accepts_zero_fractional_and_case_insensitive_iso_duration() {
        assert!(parse_share_offset_reset("by_duration:PT0S").is_ok());
        assert!(parse_share_offset_reset("by_duration:p2dt3h").is_ok());
        assert!(parse_share_offset_reset("by_duration:PT0,5S").is_ok());
    }

    #[test]
    fn rejects_missing_negative_and_calendar_durations() {
        for value in [
            "by_duration",
            "by_duration:",
            "by_duration:P",
            "by_duration:PT",
            "by_duration:P1DT",
            "by_duration:-PT1S",
            "by_duration:P1M",
            "none",
        ] {
            assert!(parse_share_offset_reset(value).is_err(), "{value}");
        }
    }
}
