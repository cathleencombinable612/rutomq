use crate::ControlError;

const MAX_NAME_LENGTH: usize = 249;

pub(crate) fn validate(name: &str) -> Result<(), ControlError> {
    let reason = if name.is_empty() {
        Some("the empty string is not allowed")
    } else if name == "." {
        Some("'.' is not allowed")
    } else if name == ".." {
        Some("'..' is not allowed")
    } else if name.chars().count() > MAX_NAME_LENGTH {
        Some("the topic name is longer than 249 characters")
    } else if !name.bytes().all(is_legal_char) {
        Some("only ASCII alphanumerics, '.', '_', and '-' are allowed")
    } else {
        None
    };
    match reason {
        Some(reason) => Err(ControlError::InvalidTopic(format!(
            "topic name '{name}' is invalid: {reason}"
        ))),
        None => Ok(()),
    }
}

pub(crate) fn collision<'a>(
    name: &str,
    mut existing: impl Iterator<Item = &'a str>,
) -> Option<&'a str> {
    if !name.contains(['.', '_']) {
        return None;
    }
    let normalized = normalize(name);
    existing.find(|candidate| *candidate != name && normalize(candidate) == normalized)
}

pub(crate) fn collision_error(name: &str, existing: &str) -> ControlError {
    ControlError::InvalidTopic(format!(
        "topic '{name}' collides with existing topic '{existing}'"
    ))
}

pub(crate) fn normalize(name: &str) -> String {
    name.replace('.', "_")
}

fn is_legal_char(value: u8) -> bool {
    value.is_ascii_alphanumeric() || matches!(value, b'.' | b'_' | b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_kafka_topic_names() {
        for name in ["events", "events.v1", "events_v1", "events-v1"] {
            assert!(validate(name).is_ok(), "{name}");
        }
        for name in ["", ".", "..", "events/v1", &"a".repeat(250)] {
            assert!(matches!(validate(name), Err(ControlError::InvalidTopic(_))));
        }
    }

    #[test]
    fn detects_dot_underscore_collisions_only_for_distinct_names() {
        let names = ["events_v1", "other"];
        assert_eq!(
            collision("events.v1", names.iter().copied()),
            Some("events_v1")
        );
        assert_eq!(collision("events_v1", names.iter().copied()), None);
        assert_eq!(collision("events-v1", names.iter().copied()), None);
    }
}
