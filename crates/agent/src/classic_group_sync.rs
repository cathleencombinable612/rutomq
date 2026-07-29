use super::Broker;
use super::authorization::{AuthorizationContext, authorization_failure};
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, NO_ERROR, REBALANCE_IN_PROGRESS, control_error_code,
};
use bytes::Bytes;
use kafka_protocol::messages::{SyncGroupRequest, SyncGroupResponse};
use rutomq_control::{AclOperation, AclResourceType, GroupAssignment};
use std::time::Duration;
use tokio::time::{Instant, sleep};

const SYNC_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const SYNC_POLL_INTERVAL: Duration = Duration::from_millis(25);

impl Broker {
    pub(super) async fn handle_sync_group(
        &self,
        request: SyncGroupRequest,
        context: &AuthorizationContext,
    ) -> SyncGroupResponse {
        let group_id = request.group_id.as_str().to_owned();
        let member_id = request.member_id.as_str().to_owned();
        let protocol_type = request.protocol_type.clone();
        let protocol_name = request.protocol_name.clone();
        if let Some((error_code, _)) = authorization_failure(
            self.authorized(
                context,
                AclResourceType::Group,
                &group_id,
                AclOperation::Read,
            )
            .await,
            GROUP_AUTHORIZATION_FAILED,
        ) {
            return response(error_code, protocol_type, protocol_name);
        }

        let follower = request.assignments.is_empty();
        let assignments: Vec<GroupAssignment> = request
            .assignments
            .into_iter()
            .map(|assignment| GroupAssignment {
                member_id: assignment.member_id.as_str().to_owned(),
                assignment: assignment.assignment.to_vec(),
            })
            .collect();
        let deadline = Instant::now() + SYNC_WAIT_TIMEOUT;
        let mut first_attempt = true;
        loop {
            let submitted = if first_attempt {
                first_attempt = false;
                assignments.clone()
            } else {
                Vec::new()
            };
            match self
                .metadata
                .sync_group(
                    &group_id,
                    request.generation_id,
                    &member_id,
                    request
                        .group_instance_id
                        .as_ref()
                        .map(|value| value.as_str()),
                    submitted,
                )
                .await
            {
                Ok(assignment) if !follower || !assignment.is_empty() => {
                    return response(NO_ERROR, protocol_type, protocol_name)
                        .with_assignment(Bytes::from(assignment));
                }
                Ok(_) if Instant::now() < deadline => sleep(SYNC_POLL_INTERVAL).await,
                Ok(_) => {
                    return response(REBALANCE_IN_PROGRESS, protocol_type, protocol_name);
                }
                Err(error) => {
                    return response(control_error_code(&error), protocol_type, protocol_name);
                }
            }
        }
    }
}

fn response(
    error_code: i16,
    protocol_type: Option<kafka_protocol::protocol::StrBytes>,
    protocol_name: Option<kafka_protocol::protocol::StrBytes>,
) -> SyncGroupResponse {
    SyncGroupResponse::default()
        .with_error_code(error_code)
        .with_protocol_type(protocol_type)
        .with_protocol_name(protocol_name)
}
