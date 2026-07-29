use chrono::{DateTime, Duration, Utc};

pub(crate) fn can_compute(
    assignment_timestamp: Option<DateTime<Utc>>,
    assignment_interval_ms: i32,
    now: DateTime<Utc>,
) -> bool {
    assignment_timestamp.is_none()
        || assignment_interval_ms == 0
        || assignment_timestamp.is_some_and(|timestamp| {
            now >= timestamp + Duration::milliseconds(i64::from(assignment_interval_ms))
        })
}
