use crate::ControlError;

pub(crate) fn validate(
    group_id: &str,
    member_id: &str,
    current: i32,
    previous: i32,
    received: i32,
    recovery_allowed: bool,
) -> Result<(), ControlError> {
    if received == current || (received < current && received == previous && recovery_allowed) {
        return Ok(());
    }
    Err(ControlError::FencedMemberEpoch {
        group: group_id.to_owned(),
        member: member_id.to_owned(),
        expected: current,
        actual: received,
    })
}

pub(crate) fn update(current: &mut i32, previous: &mut i32, next: i32) {
    if *current != next {
        *previous = *current;
        *current = next;
    }
}
