use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use super::{Broker, topic_name};
use crate::kafka_error::{CLUSTER_AUTHORIZATION_FAILED, NO_ERROR, UNKNOWN_TOPIC_OR_PARTITION};
use chrono::Utc;
use kafka_protocol::messages::describe_quorum_response::{
    Listener, Node, PartitionData, ReplicaState, TopicData,
};
use kafka_protocol::messages::{BrokerId, DescribeQuorumRequest, DescribeQuorumResponse};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{AclOperation, AclResourceType};
use uuid::Uuid;

const METADATA_TOPIC: &str = "__cluster_metadata";
const METADATA_PARTITION: i32 = 0;
pub(super) const VIRTUAL_DIRECTORY_ID: Uuid =
    Uuid::from_u128(0x00000000_0000_4000_8000_000000000001);

impl Broker {
    pub(super) async fn handle_describe_quorum(
        &self,
        request: DescribeQuorumRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> DescribeQuorumResponse {
        if !self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::Describe,
            )
            .await
            .unwrap_or(false)
        {
            return DescribeQuorumResponse::default()
                .with_error_code(CLUSTER_AUTHORIZATION_FAILED)
                .with_error_message(message("cluster authorization failed"));
        }

        if !valid_metadata_partition(&request) {
            return DescribeQuorumResponse::default()
                .with_error_code(NO_ERROR)
                .with_topics(
                    request
                        .topics
                        .into_iter()
                        .map(|topic| {
                            TopicData::default()
                                .with_topic_name(topic.topic_name)
                                .with_partitions(
                                    topic
                                        .partitions
                                        .into_iter()
                                        .map(|partition| {
                                            partition_error(
                                                partition.partition_index,
                                                UNKNOWN_TOPIC_OR_PARTITION,
                                            )
                                        })
                                        .collect(),
                                )
                        })
                        .collect(),
                );
        }

        let now_ms = Utc::now().timestamp_millis();
        let mut voter = ReplicaState::default()
            .with_replica_id(BrokerId::from(0))
            .with_log_end_offset(0)
            .with_last_fetch_timestamp(-1)
            .with_last_caught_up_timestamp(now_ms);
        if version >= 2 {
            voter = voter.with_replica_directory_id(VIRTUAL_DIRECTORY_ID);
        }
        let partition = PartitionData::default()
            .with_partition_index(METADATA_PARTITION)
            .with_error_code(NO_ERROR)
            .with_error_message(None)
            .with_leader_id(BrokerId::from(0))
            .with_leader_epoch(0)
            .with_high_watermark(0)
            .with_current_voters(vec![voter])
            .with_observers(Vec::new());
        let mut response = DescribeQuorumResponse::default()
            .with_error_code(NO_ERROR)
            .with_error_message(None)
            .with_topics(vec![
                TopicData::default()
                    .with_topic_name(topic_name(METADATA_TOPIC))
                    .with_partitions(vec![partition]),
            ]);
        if version >= 2 {
            response = response.with_nodes(vec![
                Node::default()
                    .with_node_id(BrokerId::from(0))
                    .with_listeners(vec![
                        Listener::default()
                            .with_name(string("CONTROLLER"))
                            .with_host(string(&self.config.advertise_host))
                            .with_port(
                                u16::try_from(self.config.advertise_port).unwrap_or_default(),
                            ),
                    ]),
            ]);
        }
        response
    }
}

fn valid_metadata_partition(request: &DescribeQuorumRequest) -> bool {
    request.topics.len() == 1
        && request.topics[0].topic_name.as_str() == METADATA_TOPIC
        && request.topics[0].partitions.len() == 1
        && request.topics[0].partitions[0].partition_index == METADATA_PARTITION
}

fn partition_error(partition_index: i32, error_code: i16) -> PartitionData {
    PartitionData::default()
        .with_partition_index(partition_index)
        .with_error_code(error_code)
        .with_error_message(message("unknown metadata quorum partition"))
        .with_leader_id(BrokerId::from(-1))
        .with_leader_epoch(-1)
        .with_high_watermark(-1)
}

fn string(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}

fn message(value: &str) -> Option<StrBytes> {
    Some(string(value))
}
