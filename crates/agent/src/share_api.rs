use crate::kafka_error::{
    GROUP_ID_NOT_FOUND, INVALID_RECORD_STATE, INVALID_REQUEST, INVALID_SHARE_SESSION_EPOCH,
    SHARE_SESSION_NOT_FOUND, UNKNOWN_MEMBER_ID, UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_ID,
    UNKNOWN_TOPIC_OR_PARTITION, UNSUPPORTED_VERSION,
};
use kafka_protocol::messages::GroupId;
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{ControlError, SHARE_VERSION_FEATURE};

use super::Broker;

pub(super) struct ShareIdentity {
    pub group_id: String,
    pub member_id: String,
}

pub(super) fn identity(
    group_id: &Option<GroupId>,
    member_id: &Option<StrBytes>,
) -> Result<ShareIdentity, &'static str> {
    let group_id = group_id
        .as_ref()
        .map(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or("share group ID is required")?;
    let member_id = member_id
        .as_ref()
        .map(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or("share group member ID is required")?;
    Ok(ShareIdentity {
        group_id: group_id.to_owned(),
        member_id: member_id.to_owned(),
    })
}

pub(super) fn error_code(error: &ControlError) -> i16 {
    match error {
        ControlError::GroupNotFound(_) => GROUP_ID_NOT_FOUND,
        ControlError::GroupMemberNotFound { .. } => UNKNOWN_MEMBER_ID,
        ControlError::ShareSessionNotFound { .. } => SHARE_SESSION_NOT_FOUND,
        ControlError::InvalidShareSessionEpoch { .. } => INVALID_SHARE_SESSION_EPOCH,
        ControlError::InvalidShareRecordState(_) => INVALID_RECORD_STATE,
        ControlError::TopicNotFound(_) => UNKNOWN_TOPIC_ID,
        ControlError::PartitionNotFound { .. } => UNKNOWN_TOPIC_OR_PARTITION,
        ControlError::InvalidRequest(_) => INVALID_REQUEST,
        _ => UNKNOWN_SERVER_ERROR,
    }
}

pub(super) fn string(value: impl Into<String>) -> StrBytes {
    StrBytes::from_string(value.into())
}

impl Broker {
    pub(super) async fn share_feature_error(&self) -> Option<(i16, String)> {
        match self.metadata.features().await {
            Ok(features) if features.level(SHARE_VERSION_FEATURE) < 1 => Some((
                UNSUPPORTED_VERSION,
                "share groups are disabled by share.version".to_owned(),
            )),
            Err(error) => Some((UNKNOWN_SERVER_ERROR, error.to_string())),
            _ => None,
        }
    }
}
