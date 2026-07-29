use super::authorization::CLUSTER_RESOURCE_NAME;
use super::*;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, ELECTION_NOT_NEEDED, INVALID_REPLICA_ASSIGNMENT, INVALID_REQUEST,
    NO_REASSIGNMENT_IN_PROGRESS,
};
use kafka_protocol::messages::alter_partition_reassignments_response::{
    ReassignablePartitionResponse, ReassignableTopicResponse,
};
use kafka_protocol::messages::elect_leaders_response::{PartitionResult, ReplicaElectionResult};

impl Broker {
    pub(super) async fn handle_elect_leaders(
        &self,
        request: ElectLeadersRequest,
        context: &AuthorizationContext,
    ) -> ElectLeadersResponse {
        if !self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::Alter,
            )
            .await
            .unwrap_or(false)
        {
            return ElectLeadersResponse::default().with_error_code(CLUSTER_AUTHORIZATION_FAILED);
        }
        if !matches!(request.election_type, 0 | 1) {
            return ElectLeadersResponse::default().with_error_code(INVALID_REQUEST);
        }

        let results = if let Some(topics) = request.topic_partitions {
            let mut results = Vec::with_capacity(topics.len());
            for topic in topics {
                let info = self.metadata.topic(topic.topic.as_str()).await;
                let mut partitions = Vec::with_capacity(topic.partitions.len());
                for partition in topic.partitions {
                    let (error_code, message) = match &info {
                        Ok(Some(info)) if partition >= 0 && partition < info.partitions => (
                            ELECTION_NOT_NEEDED,
                            Some("the virtual broker is already the leader"),
                        ),
                        Ok(_) => (
                            UNKNOWN_TOPIC_OR_PARTITION,
                            Some("topic or partition was not found"),
                        ),
                        Err(_) => (UNKNOWN_SERVER_ERROR, Some("metadata lookup failed")),
                    };
                    partitions.push(
                        PartitionResult::default()
                            .with_partition_id(partition)
                            .with_error_code(error_code)
                            .with_error_message(
                                message.map(|message| StrBytes::from_string(message.to_owned())),
                            ),
                    );
                }
                results.push(
                    ReplicaElectionResult::default()
                        .with_topic(topic.topic)
                        .with_partition_result(partitions),
                );
            }
            results
        } else {
            match self.metadata.topics(None).await {
                Ok(topics) => topics
                    .into_iter()
                    .map(|topic| {
                        ReplicaElectionResult::default()
                            .with_topic(topic_name(&topic.name))
                            .with_partition_result(Vec::new())
                    })
                    .collect(),
                Err(_) => {
                    return ElectLeadersResponse::default().with_error_code(UNKNOWN_SERVER_ERROR);
                }
            }
        };
        ElectLeadersResponse::default()
            .with_error_code(NO_ERROR)
            .with_replica_election_results(results)
    }

    pub(super) async fn handle_alter_partition_reassignments(
        &self,
        request: AlterPartitionReassignmentsRequest,
        context: &AuthorizationContext,
    ) -> AlterPartitionReassignmentsResponse {
        let allow_replication_factor_change = request.allow_replication_factor_change;
        if !self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::Alter,
            )
            .await
            .unwrap_or(false)
        {
            return AlterPartitionReassignmentsResponse::default()
                .with_allow_replication_factor_change(allow_replication_factor_change)
                .with_error_code(CLUSTER_AUTHORIZATION_FAILED)
                .with_error_message(Some(StrBytes::from_string(
                    "cluster authorization failed".to_owned(),
                )));
        }

        let mut responses = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let info = self.metadata.topic(topic.name.as_str()).await;
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let (error_code, message) = match &info {
                    Ok(Some(info))
                        if partition.partition_index >= 0
                            && partition.partition_index < info.partitions =>
                    {
                        match partition.replicas.as_deref() {
                            None => (
                                NO_REASSIGNMENT_IN_PROGRESS,
                                Some("no reassignment is in progress"),
                            ),
                            Some(replicas)
                                if replicas.len() == 1 && replicas[0] == BrokerId::from(0) =>
                            {
                                (NO_ERROR, None)
                            }
                            Some(_) => (
                                INVALID_REPLICA_ASSIGNMENT,
                                Some("rutomq partitions have the virtual replica set [0]"),
                            ),
                        }
                    }
                    Ok(_) => (
                        UNKNOWN_TOPIC_OR_PARTITION,
                        Some("topic or partition was not found"),
                    ),
                    Err(_) => (UNKNOWN_SERVER_ERROR, Some("metadata lookup failed")),
                };
                partitions.push(
                    ReassignablePartitionResponse::default()
                        .with_partition_index(partition.partition_index)
                        .with_error_code(error_code)
                        .with_error_message(
                            message.map(|message| StrBytes::from_string(message.to_owned())),
                        ),
                );
            }
            responses.push(
                ReassignableTopicResponse::default()
                    .with_name(topic.name)
                    .with_partitions(partitions),
            );
        }
        AlterPartitionReassignmentsResponse::default()
            .with_allow_replication_factor_change(allow_replication_factor_change)
            .with_error_code(NO_ERROR)
            .with_error_message(None)
            .with_responses(responses)
    }

    pub(super) async fn handle_list_partition_reassignments(
        &self,
        _request: ListPartitionReassignmentsRequest,
        context: &AuthorizationContext,
    ) -> ListPartitionReassignmentsResponse {
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
            return ListPartitionReassignmentsResponse::default()
                .with_error_code(CLUSTER_AUTHORIZATION_FAILED)
                .with_error_message(Some(StrBytes::from_string(
                    "cluster authorization failed".to_owned(),
                )));
        }
        ListPartitionReassignmentsResponse::default()
            .with_error_code(NO_ERROR)
            .with_error_message(None)
            .with_topics(Vec::new())
    }
}
