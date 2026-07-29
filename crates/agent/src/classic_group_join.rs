use super::Broker;
use super::authorization::{AuthorizationContext, authorization_failure};
use crate::kafka_error::{
    GROUP_AUTHORIZATION_FAILED, INVALID_SESSION_TIMEOUT, MEMBER_ID_REQUIRED, NO_ERROR,
    REBALANCE_IN_PROGRESS, control_error_code,
};
use bytes::Bytes;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::{JoinGroupRequest, JoinGroupResponse};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{AclOperation, AclResourceType, ControlError, JoinGroupResult};
use std::time::Duration;
use tokio::time::{Instant, sleep};

const MAX_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DEADLINE_GRACE: Duration = Duration::from_millis(250);

impl Broker {
    pub(super) async fn handle_join_group(
        &self,
        request: JoinGroupRequest,
        version: i16,
        context: &AuthorizationContext,
        client_id: &str,
    ) -> JoinGroupResponse {
        let requested_member_id = request.member_id.as_str().to_owned();
        let group_id = request.group_id.as_str().to_owned();
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
            return JoinGroupResponse::default()
                .with_error_code(error_code)
                .with_member_id(StrBytes::from_string(requested_member_id));
        }

        if request.session_timeout_ms < self.config.classic_group_min_session_timeout_ms
            || request.session_timeout_ms > self.config.classic_group_max_session_timeout_ms
        {
            return JoinGroupResponse::default()
                .with_error_code(INVALID_SESSION_TIMEOUT)
                .with_member_id(StrBytes::from_string(requested_member_id));
        }

        let group_instance_id = request
            .group_instance_id
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let protocol_type = request.protocol_type.as_str().to_owned();
        let protocols = request
            .protocols
            .into_iter()
            .map(|protocol| {
                (
                    protocol.name.as_str().to_owned(),
                    protocol.metadata.to_vec(),
                )
            })
            .collect::<Vec<_>>();
        let subscribed_topics =
            super::classic_group_subscription::subscribed_topics(&protocol_type, &protocols);
        let rebalance_timeout_ms = if request.rebalance_timeout_ms > 0 {
            request.rebalance_timeout_ms
        } else {
            request.session_timeout_ms
        };
        let deadline =
            Instant::now() + Duration::from_millis(rebalance_timeout_ms as u64) + DEADLINE_GRACE;
        let result = self
            .metadata
            .begin_join_group(
                &group_id,
                &requested_member_id,
                group_instance_id.as_deref(),
                &protocol_type,
                &protocols,
                (
                    client_id,
                    &context.host,
                    &subscribed_topics,
                    request.session_timeout_ms,
                ),
                rebalance_timeout_ms,
                self.config.classic_group_initial_rebalance_delay_ms,
                self.config.classic_group_max_size,
                version,
            )
            .await;
        self.await_join_result(
            result,
            &group_id,
            group_instance_id.as_deref(),
            version,
            deadline,
        )
        .await
    }

    async fn await_join_result(
        &self,
        mut result: Result<JoinGroupResult, ControlError>,
        group_id: &str,
        group_instance_id: Option<&str>,
        version: i16,
        deadline: Instant,
    ) -> JoinGroupResponse {
        let mut final_poll_done = false;
        loop {
            match result {
                Ok(joined) => {
                    let Some(rebalance_id) = joined.pending_rebalance else {
                        return response(joined, version);
                    };
                    if Instant::now() >= deadline {
                        if final_poll_done {
                            return JoinGroupResponse::default()
                                .with_error_code(REBALANCE_IN_PROGRESS)
                                .with_member_id(StrBytes::from_string(joined.member_id));
                        }
                        final_poll_done = true;
                    } else {
                        let delay = Duration::from_millis(joined.retry_after_ms.max(1) as u64)
                            .min(MAX_POLL_INTERVAL);
                        sleep(delay).await;
                    }
                    result = self
                        .metadata
                        .poll_join_group(
                            group_id,
                            &joined.member_id,
                            group_instance_id,
                            rebalance_id,
                            version,
                        )
                        .await;
                }
                Err(ControlError::MemberIdRequired { member_id }) => {
                    return JoinGroupResponse::default()
                        .with_error_code(MEMBER_ID_REQUIRED)
                        .with_member_id(StrBytes::from_string(member_id));
                }
                Err(error) => {
                    return JoinGroupResponse::default()
                        .with_error_code(control_error_code(&error));
                }
            }
        }
    }
}

fn response(result: JoinGroupResult, version: i16) -> JoinGroupResponse {
    let protocol_type = result.protocol_type.clone();
    let members = if result.member_id == result.leader {
        result.members
    } else {
        Vec::new()
    };
    let response = JoinGroupResponse::default()
        .with_error_code(NO_ERROR)
        .with_generation_id(result.generation_id)
        .with_protocol_name(Some(StrBytes::from_string(result.protocol_name)))
        .with_leader(StrBytes::from_string(result.leader))
        .with_member_id(StrBytes::from_string(result.member_id))
        .with_skip_assignment(result.skip_assignment)
        .with_members(
            members
                .into_iter()
                .map(|member| {
                    JoinGroupResponseMember::default()
                        .with_member_id(StrBytes::from_string(member.member_id))
                        .with_group_instance_id(if version >= 5 {
                            member.group_instance_id.map(StrBytes::from_string)
                        } else {
                            None
                        })
                        .with_metadata(Bytes::from(member.metadata))
                })
                .collect(),
        );
    if version >= 7 {
        response.with_protocol_type(Some(StrBytes::from_string(protocol_type)))
    } else {
        response
    }
}
