use super::*;
use crate::kafka_error::{
    GROUP_ID_NOT_FOUND, MISMATCHED_ENDPOINT_TYPE, NON_EMPTY_GROUP, UNKNOWN_SERVER_ERROR,
};
use bytes::Buf;
use kafka_protocol::messages::{
    DeleteGroupsRequest, DeleteGroupsResponse, DescribeClusterRequest, DescribeClusterResponse,
    DescribeGroupsRequest, DescribeGroupsResponse, GroupId, ListGroupsRequest, ListGroupsResponse,
    RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use rutomq_control::{GroupAssignment, MemoryMetadataStore, OffsetCommit, PostgresMetadataStore};
use rutomq_storage::OpenDalObjectStore;
use uuid::Uuid;

fn test_broker() -> Broker {
    broker_with_metadata(Arc::new(MemoryMetadataStore::new()))
}

fn broker_with_metadata(metadata: Arc<dyn MetadataStore>) -> Broker {
    Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig::default(),
        Arc::new(Metrics::new().unwrap()),
    )
}

fn group_id(value: &str) -> GroupId {
    GroupId::from(StrBytes::from_string(value.to_owned()))
}

fn request_frame<T: Encodable>(api_key: ApiKey, version: i16, body: &T) -> Bytes {
    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(42)
        .with_client_id(Some(StrBytes::from_string("admin-test".to_owned())))
        .encode(&mut payload, api_key.request_header_version(version))
        .unwrap();
    body.encode(&mut payload, version).unwrap();
    payload.freeze()
}

fn decode_response<T: Decodable>(api_key: ApiKey, version: i16, mut frame: Bytes) -> T {
    let frame_size = frame.get_i32() as usize;
    assert_eq!(frame_size, frame.remaining());
    ResponseHeader::decode(&mut frame, api_key.response_header_version(version)).unwrap();
    T::decode(&mut frame, version).unwrap()
}

#[tokio::test]
async fn classic_group_admin_apis_round_trip() {
    let broker = test_broker();
    broker.metadata.create_topic("events", 1).await.unwrap();
    let joined = broker
        .metadata
        .join_group(
            "classic-workers",
            "",
            Some("instance-a"),
            "consumer",
            &[("range".to_owned(), vec![1, 2, 3])],
            (
                "classic-client",
                "127.0.0.1",
                &["events".to_owned()],
                45_000,
            ),
            9,
        )
        .await
        .unwrap();
    broker
        .metadata
        .sync_group(
            "classic-workers",
            joined.generation_id,
            &joined.member_id,
            Some("instance-a"),
            vec![GroupAssignment {
                member_id: joined.member_id.clone(),
                assignment: vec![4, 5, 6],
            }],
        )
        .await
        .unwrap();

    let list = ListGroupsRequest::default()
        .with_states_filter(vec![StrBytes::from_string("stable".to_owned())])
        .with_types_filter(vec![StrBytes::from_string("classic".to_owned())]);
    let response = broker
        .handle_request(request_frame(ApiKey::ListGroups, 5, &list))
        .await
        .unwrap();
    let response: ListGroupsResponse = decode_response(ApiKey::ListGroups, 5, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.groups.len(), 1);
    assert_eq!(response.groups[0].group_state.as_str(), "Stable");
    assert_eq!(response.groups[0].group_type.as_str(), "Classic");

    let describe = DescribeGroupsRequest::default()
        .with_groups(vec![group_id("classic-workers")])
        .with_include_authorized_operations(true);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeGroups, 6, &describe))
        .await
        .unwrap();
    let response: DescribeGroupsResponse = decode_response(ApiKey::DescribeGroups, 6, response);
    let described = &response.groups[0];
    assert_eq!(described.error_code, NO_ERROR);
    assert_eq!(described.group_state.as_str(), "Stable");
    assert_eq!(described.protocol_data.as_str(), "range");
    assert_eq!(described.members[0].client_id.as_str(), "classic-client");
    assert_eq!(described.members[0].member_assignment.as_ref(), [4, 5, 6]);
    assert_ne!(described.authorized_operations, i32::MIN);

    let delete =
        DeleteGroupsRequest::default().with_groups_names(vec![group_id("classic-workers")]);
    let response = broker
        .handle_request(request_frame(ApiKey::DeleteGroups, 2, &delete))
        .await
        .unwrap();
    let response: DeleteGroupsResponse = decode_response(ApiKey::DeleteGroups, 2, response);
    assert_eq!(response.results[0].error_code, NON_EMPTY_GROUP);

    broker
        .metadata
        .commit_offsets(
            "offset-only",
            vec![OffsetCommit {
                partition: PartitionKey::new("events", 0),
                offset: 0,
                leader_epoch: -1,
                metadata: None,
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    let delete = DeleteGroupsRequest::default().with_groups_names(vec![group_id("offset-only")]);
    let response = broker
        .handle_request(request_frame(ApiKey::DeleteGroups, 2, &delete))
        .await
        .unwrap();
    let response: DeleteGroupsResponse = decode_response(ApiKey::DeleteGroups, 2, response);
    assert_eq!(response.results[0].error_code, NO_ERROR);
    let response = broker
        .handle_request(request_frame(ApiKey::DeleteGroups, 2, &delete))
        .await
        .unwrap();
    let response: DeleteGroupsResponse = decode_response(ApiKey::DeleteGroups, 2, response);
    assert_eq!(response.results[0].error_code, GROUP_ID_NOT_FOUND);
}

#[tokio::test]
async fn delete_groups_processes_each_distinct_group_once() {
    assert_distinct_group_deletion(Arc::new(MemoryMetadataStore::new()), "memory").await;
}

#[tokio::test]
async fn postgres_delete_groups_deduplicates_before_mutation() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let setup = PostgresMetadataStore::connect(&database_url).await.unwrap();
    setup.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let metadata: Arc<dyn MetadataStore> = Arc::new(setup);
    assert_distinct_group_deletion(metadata, &suffix).await;
}

#[tokio::test]
async fn delete_groups_authorization_backend_failure_is_a_server_error() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    let broker = Broker::new(
        metadata.clone(),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    metadata.set_authorization_failure(true);

    let request =
        DeleteGroupsRequest::default().with_groups_names(vec![group_id("backend-failure")]);
    let response = broker
        .handle_request(request_frame(ApiKey::DeleteGroups, 2, &request))
        .await
        .unwrap();
    let response: DeleteGroupsResponse = decode_response(ApiKey::DeleteGroups, 2, response);
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].error_code, UNKNOWN_SERVER_ERROR);
}

async fn assert_distinct_group_deletion(metadata: Arc<dyn MetadataStore>, suffix: &str) {
    let topic = format!("delete-groups-topic-{suffix}");
    let duplicate = format!("delete-groups-duplicate-{suffix}");
    let unique = format!("delete-groups-unique-{suffix}");
    metadata.create_topic(&topic, 1).await.unwrap();
    for group in [&duplicate, &unique] {
        metadata
            .commit_offsets(
                group,
                vec![OffsetCommit {
                    partition: PartitionKey::new(&topic, 0),
                    offset: 0,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                }],
            )
            .await
            .unwrap();
    }
    let broker = broker_with_metadata(metadata);
    let request = DeleteGroupsRequest::default().with_groups_names(vec![
        group_id(&duplicate),
        group_id(&unique),
        group_id(&duplicate),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DeleteGroups, 2, &request))
        .await
        .unwrap();
    let response: DeleteGroupsResponse = decode_response(ApiKey::DeleteGroups, 2, response);
    assert_eq!(response.results.len(), 2);
    assert_eq!(response.results[0].group_id.as_str(), duplicate);
    assert_eq!(response.results[0].error_code, NO_ERROR);
    assert_eq!(response.results[1].group_id.as_str(), unique);
    assert_eq!(response.results[1].error_code, NO_ERROR);
}

#[tokio::test]
async fn describe_cluster_reports_virtual_broker() {
    let broker = test_broker();
    let request = DescribeClusterRequest::default()
        .with_endpoint_type(1)
        .with_include_cluster_authorized_operations(true);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeCluster, 2, &request))
        .await
        .unwrap();
    let response: DescribeClusterResponse = decode_response(ApiKey::DescribeCluster, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.cluster_id.as_str(), "rutomq-cluster");
    assert_eq!(response.controller_id, BrokerId::from(0));
    assert_eq!(response.brokers.len(), 1);
    assert_eq!(response.brokers[0].broker_id, BrokerId::from(0));
    assert_eq!(response.brokers[0].host.as_str(), "127.0.0.1");
    assert_ne!(response.cluster_authorized_operations, i32::MIN);

    let unsupported = DescribeClusterRequest::default().with_endpoint_type(2);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeCluster, 2, &unsupported))
        .await
        .unwrap();
    let response: DescribeClusterResponse = decode_response(ApiKey::DescribeCluster, 2, response);
    assert_eq!(response.error_code, MISMATCHED_ENDPOINT_TYPE);
}
