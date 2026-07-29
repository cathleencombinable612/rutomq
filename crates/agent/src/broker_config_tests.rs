use super::tests::{decode_response, request_frame};
use super::*;
use crate::kafka_error::{CLUSTER_AUTHORIZATION_FAILED, INVALID_REQUEST, NO_ERROR};
use crate::server::broker_config::{
    ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MAX_MS, ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS,
    GROUP_CONSUMER_ASSIGNORS, GROUP_CONSUMER_MAX_HEARTBEAT_INTERVAL_MS,
    GROUP_CONSUMER_MAX_SESSION_TIMEOUT_MS, GROUP_CONSUMER_MAX_SIZE,
    GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS, GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS,
    GROUP_CONSUMER_REGEX_REFRESH_INTERVAL_MS, GROUP_COORDINATOR_REBALANCE_PROTOCOLS,
    GROUP_MAX_SESSION_TIMEOUT_MS, GROUP_MAX_SIZE, GROUP_MIN_SESSION_TIMEOUT_MS,
    GROUP_SHARE_ASSIGNORS, GROUP_SHARE_MAX_HEARTBEAT_INTERVAL_MS,
    GROUP_SHARE_MAX_RECORD_LOCK_DURATION_MS, GROUP_SHARE_MAX_SESSION_TIMEOUT_MS,
    GROUP_SHARE_MAX_SIZE, GROUP_SHARE_MIN_HEARTBEAT_INTERVAL_MS,
    GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS, GROUP_SHARE_MIN_SESSION_TIMEOUT_MS,
    GROUP_STREAMS_INITIAL_REBALANCE_DELAY_MS, GROUP_STREAMS_MAX_HEARTBEAT_INTERVAL_MS,
    GROUP_STREAMS_MAX_SESSION_TIMEOUT_MS, GROUP_STREAMS_MAX_SIZE,
    GROUP_STREAMS_MAX_STANDBY_REPLICAS, GROUP_STREAMS_MIN_HEARTBEAT_INTERVAL_MS,
    GROUP_STREAMS_MIN_SESSION_TIMEOUT_MS, OFFSET_METADATA_MAX_BYTES,
    OFFSETS_RETENTION_CHECK_INTERVAL_MS, OFFSETS_RETENTION_MINUTES,
    TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS, TRANSACTION_MAX_TIMEOUT_MS,
    TRANSACTION_PARTITION_VERIFICATION_ENABLE,
    TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS,
    TRANSACTION_TWO_PHASE_COMMIT_ENABLE, TRANSACTIONAL_ID_EXPIRATION_MS,
};
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::{DescribeConfigsRequest, DescribeConfigsResponse};
use rutomq_control::MemoryMetadataStore;
use rutomq_storage::OpenDalObjectStore;

fn configured_broker() -> Broker {
    let config = AgentConfig {
        kafka_addr: "127.0.0.1:19092".parse().unwrap(),
        advertise_host: "kafka.rutomq.test".to_owned(),
        advertise_port: 9092,
        max_frame_size: 12_345_678,
        max_request_partition_size_limit: 321,
        log_filter: "rutomq=debug,tower_http=warn".to_owned(),
        num_partitions: 3,
        auto_create_topics_enable: false,
        ..AgentConfig::default()
    };
    Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

fn describe_resource(
    resource_type: i8,
    resource_name: &str,
    keys: Option<Vec<&str>>,
) -> DescribeConfigsResource {
    DescribeConfigsResource::default()
        .with_resource_type(resource_type)
        .with_resource_name(StrBytes::from_string(resource_name.to_owned()))
        .with_configuration_keys(keys.map(|keys| {
            keys.into_iter()
                .map(|key| StrBytes::from_string(key.to_owned()))
                .collect()
        }))
}

#[tokio::test]
async fn describes_virtual_broker_and_logger_configs_across_versions() {
    let broker = configured_broker();
    for version in 1..=4 {
        let mut request = DescribeConfigsRequest::default()
            .with_resources(vec![
                describe_resource(
                    4,
                    "0",
                    Some(vec![
                        "node.id",
                        "listeners",
                        "socket.request.max.bytes",
                        "connections.max.reauth.ms",
                        "max.request.partition.size.limit",
                        "num.partitions",
                        "default.replication.factor",
                        "auto.create.topics.enable",
                        "producer.id.expiration.ms",
                        "offset.metadata.max.bytes",
                        "offsets.retention.minutes",
                        "offsets.retention.check.interval.ms",
                        "transactional.id.expiration.ms",
                        "transaction.remove.expired.transaction.cleanup.interval.ms",
                        "transaction.abort.timed.out.transaction.cleanup.interval.ms",
                        "add.partitions.to.txn.retry.backoff.ms",
                        "add.partitions.to.txn.retry.backoff.max.ms",
                        "transaction.partition.verification.enable",
                        "transaction.max.timeout.ms",
                        "transaction.two.phase.commit.enable",
                        "group.min.session.timeout.ms",
                        "group.max.session.timeout.ms",
                        "group.max.size",
                        "group.consumer.min.heartbeat.interval.ms",
                        "group.consumer.max.heartbeat.interval.ms",
                        "group.consumer.min.session.timeout.ms",
                        "group.consumer.max.session.timeout.ms",
                        "group.consumer.max.size",
                        "group.consumer.assignors",
                        "group.consumer.regex.refresh.interval.ms",
                        "group.streams.min.heartbeat.interval.ms",
                        "group.streams.max.heartbeat.interval.ms",
                        "group.streams.min.session.timeout.ms",
                        "group.streams.max.session.timeout.ms",
                        "group.streams.max.size",
                        "group.streams.max.standby.replicas",
                        "group.streams.initial.rebalance.delay.ms",
                        "group.share.min.heartbeat.interval.ms",
                        "group.share.max.heartbeat.interval.ms",
                        "group.share.min.session.timeout.ms",
                        "group.share.max.session.timeout.ms",
                        "group.share.max.size",
                        "group.share.assignors",
                        "group.share.min.record.lock.duration.ms",
                        "group.share.max.record.lock.duration.ms",
                        "group.coordinator.rebalance.protocols",
                        "group.share.delivery.count.limit",
                        "group.share.min.delivery.count.limit",
                        "group.share.max.delivery.count.limit",
                        "group.share.partition.max.record.locks",
                        "group.share.min.partition.max.record.locks",
                        "group.share.max.partition.max.record.locks",
                        "unknown.key",
                    ]),
                ),
                describe_resource(4, "", None),
                describe_resource(8, "0", Some(vec!["rutomq.tracing.filter"])),
            ])
            .with_include_synonyms(true);
        if version >= 3 {
            request = request.with_include_documentation(true);
        }
        let response = broker
            .handle_request(request_frame(
                ApiKey::DescribeConfigs,
                version,
                550 + i32::from(version),
                &request,
            ))
            .await
            .unwrap();
        let response: DescribeConfigsResponse =
            decode_response(ApiKey::DescribeConfigs, version, response);
        assert_eq!(response.results.len(), 3);

        let broker_result = &response.results[0];
        assert_eq!(broker_result.error_code, NO_ERROR);
        assert_eq!(broker_result.configs.len(), 52);
        let node_id = broker_result
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == "node.id")
            .unwrap();
        assert_eq!(node_id.value.as_ref().unwrap().as_str(), "0");
        assert!(node_id.read_only);
        assert_eq!(node_id.config_source, 4);
        assert_eq!(node_id.synonyms.len(), 1);
        assert_eq!(node_id.synonyms[0].source, 4);
        let max_reauth = broker_result
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == "connections.max.reauth.ms")
            .unwrap();
        assert_eq!(max_reauth.value.as_ref().unwrap().as_str(), "0");
        assert!(max_reauth.read_only);
        assert_eq!(max_reauth.config_source, 4);
        assert_eq!(max_reauth.synonyms.len(), 1);
        assert_eq!(
            broker_result
                .configs
                .iter()
                .find(|entry| entry.name.as_str() == "listeners")
                .unwrap()
                .value
                .as_ref()
                .unwrap()
                .as_str(),
            "PLAINTEXT://127.0.0.1:19092"
        );
        assert_eq!(
            broker_result
                .configs
                .iter()
                .find(|entry| entry.name.as_str() == "socket.request.max.bytes")
                .unwrap()
                .value
                .as_ref()
                .unwrap()
                .as_str(),
            "12345678"
        );
        assert_eq!(
            broker_result
                .configs
                .iter()
                .find(|entry| entry.name.as_str() == "max.request.partition.size.limit")
                .unwrap()
                .value
                .as_ref()
                .unwrap()
                .as_str(),
            "321"
        );
        for (name, expected) in [
            ("num.partitions", "3"),
            ("default.replication.factor", "1"),
            ("auto.create.topics.enable", "false"),
        ] {
            let entry = broker_result
                .configs
                .iter()
                .find(|entry| entry.name.as_str() == name)
                .unwrap();
            assert_eq!(entry.value.as_ref().unwrap().as_str(), expected);
            assert!(entry.read_only);
            assert_eq!(entry.config_source, 4);
        }
        assert_eq!(
            broker_result
                .configs
                .iter()
                .find(|entry| entry.name.as_str() == "producer.id.expiration.ms")
                .unwrap()
                .value
                .as_ref()
                .unwrap()
                .as_str(),
            "86400000"
        );
        for (name, expected) in [
            (OFFSET_METADATA_MAX_BYTES, "4096"),
            (OFFSETS_RETENTION_MINUTES, "10080"),
            (OFFSETS_RETENTION_CHECK_INTERVAL_MS, "600000"),
        ] {
            let entry = broker_result
                .configs
                .iter()
                .find(|entry| entry.name.as_str() == name)
                .unwrap();
            assert_eq!(entry.value.as_ref().unwrap().as_str(), expected);
            assert!(entry.read_only);
            assert_eq!(entry.config_source, 4);
            assert_eq!(entry.synonyms.len(), 1);
            assert_eq!(entry.synonyms[0].name.as_str(), name);
            assert_eq!(entry.synonyms[0].source, 4);
        }
        let transactional_id_expiration = broker_result
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == TRANSACTIONAL_ID_EXPIRATION_MS)
            .unwrap();
        assert_eq!(
            transactional_id_expiration.value.as_ref().unwrap().as_str(),
            "604800000"
        );
        assert!(transactional_id_expiration.read_only);
        assert_eq!(transactional_id_expiration.config_source, 4);
        assert_eq!(transactional_id_expiration.synonyms.len(), 1);
        assert_eq!(transactional_id_expiration.synonyms[0].source, 4);
        let transactional_id_cleanup = broker_result
            .configs
            .iter()
            .find(|entry| {
                entry.name.as_str() == TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS
            })
            .unwrap();
        assert_eq!(
            transactional_id_cleanup.value.as_ref().unwrap().as_str(),
            "3600000"
        );
        assert!(transactional_id_cleanup.read_only);
        assert_eq!(transactional_id_cleanup.config_source, 4);
        assert_eq!(transactional_id_cleanup.synonyms.len(), 1);
        assert_eq!(transactional_id_cleanup.synonyms[0].source, 4);
        let transaction_abort_cleanup = broker_result
            .configs
            .iter()
            .find(|entry| {
                entry.name.as_str() == TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS
            })
            .unwrap();
        assert_eq!(
            transaction_abort_cleanup.value.as_ref().unwrap().as_str(),
            "10000"
        );
        assert!(transaction_abort_cleanup.read_only);
        assert_eq!(transaction_abort_cleanup.config_source, 4);
        assert_eq!(transaction_abort_cleanup.synonyms.len(), 1);
        assert_eq!(transaction_abort_cleanup.synonyms[0].source, 4);
        let transaction_retry_backoffs = [
            (
                ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS,
                "20",
                broker_result
                    .configs
                    .iter()
                    .find(|entry| entry.name.as_str() == ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS)
                    .unwrap(),
            ),
            (
                ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MAX_MS,
                "100",
                broker_result
                    .configs
                    .iter()
                    .find(|entry| entry.name.as_str() == ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MAX_MS)
                    .unwrap(),
            ),
        ];
        for (name, expected, entry) in transaction_retry_backoffs {
            assert_eq!(entry.value.as_ref().unwrap().as_str(), expected);
            assert!(entry.read_only);
            assert_eq!(entry.config_source, 4);
            assert_eq!(entry.synonyms.len(), 1);
            assert_eq!(entry.synonyms[0].name.as_str(), name);
            assert_eq!(entry.synonyms[0].source, 4);
        }
        let transaction_partition_verification = broker_result
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == TRANSACTION_PARTITION_VERIFICATION_ENABLE)
            .unwrap();
        assert_eq!(
            transaction_partition_verification
                .value
                .as_ref()
                .unwrap()
                .as_str(),
            "true"
        );
        assert!(!transaction_partition_verification.read_only);
        assert_eq!(transaction_partition_verification.config_source, 4);
        assert_eq!(transaction_partition_verification.synonyms.len(), 1);
        assert_eq!(transaction_partition_verification.synonyms[0].source, 4);
        let transaction_max_timeout = broker_result
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == TRANSACTION_MAX_TIMEOUT_MS)
            .unwrap();
        assert_eq!(
            transaction_max_timeout.value.as_ref().unwrap().as_str(),
            "900000"
        );
        assert!(transaction_max_timeout.read_only);
        assert_eq!(transaction_max_timeout.config_source, 4);
        assert_eq!(transaction_max_timeout.synonyms.len(), 1);
        assert_eq!(transaction_max_timeout.synonyms[0].source, 4);
        let two_phase_commit = broker_result
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == TRANSACTION_TWO_PHASE_COMMIT_ENABLE)
            .unwrap();
        assert_eq!(two_phase_commit.value.as_ref().unwrap().as_str(), "false");
        assert!(two_phase_commit.read_only);
        assert_eq!(two_phase_commit.config_source, 4);
        assert_eq!(two_phase_commit.synonyms.len(), 1);
        assert_eq!(two_phase_commit.synonyms[0].source, 4);
        for (name, expected) in [
            (GROUP_MIN_SESSION_TIMEOUT_MS, "6000"),
            (GROUP_MAX_SESSION_TIMEOUT_MS, "1800000"),
            (GROUP_MAX_SIZE, "2147483647"),
            (GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS, "5000"),
            (GROUP_CONSUMER_MAX_HEARTBEAT_INTERVAL_MS, "15000"),
            (GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS, "45000"),
            (GROUP_CONSUMER_MAX_SESSION_TIMEOUT_MS, "60000"),
            (GROUP_CONSUMER_MAX_SIZE, "2147483647"),
            (GROUP_CONSUMER_ASSIGNORS, "uniform,range"),
            (GROUP_CONSUMER_REGEX_REFRESH_INTERVAL_MS, "600000"),
            (GROUP_STREAMS_MIN_HEARTBEAT_INTERVAL_MS, "5000"),
            (GROUP_STREAMS_MAX_HEARTBEAT_INTERVAL_MS, "15000"),
            (GROUP_STREAMS_MIN_SESSION_TIMEOUT_MS, "45000"),
            (GROUP_STREAMS_MAX_SESSION_TIMEOUT_MS, "60000"),
            (GROUP_STREAMS_MAX_SIZE, "2147483647"),
            (GROUP_STREAMS_MAX_STANDBY_REPLICAS, "2"),
            (GROUP_STREAMS_INITIAL_REBALANCE_DELAY_MS, "3000"),
            (GROUP_SHARE_MIN_HEARTBEAT_INTERVAL_MS, "5000"),
            (GROUP_SHARE_MAX_HEARTBEAT_INTERVAL_MS, "15000"),
            (GROUP_SHARE_MIN_SESSION_TIMEOUT_MS, "45000"),
            (GROUP_SHARE_MAX_SESSION_TIMEOUT_MS, "60000"),
            (GROUP_SHARE_MAX_SIZE, "200"),
            (GROUP_SHARE_ASSIGNORS, "simple"),
            (GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS, "15000"),
            (GROUP_SHARE_MAX_RECORD_LOCK_DURATION_MS, "60000"),
        ] {
            let entry = broker_result
                .configs
                .iter()
                .find(|entry| entry.name.as_str() == name)
                .unwrap();
            assert_eq!(entry.value.as_ref().unwrap().as_str(), expected);
            assert!(entry.read_only);
            assert_eq!(entry.config_source, 4);
            assert_eq!(entry.synonyms.len(), 1);
            assert_eq!(entry.synonyms[0].name.as_str(), name);
            assert_eq!(entry.synonyms[0].source, 4);
        }
        let rebalance_protocols = broker_result
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == GROUP_COORDINATOR_REBALANCE_PROTOCOLS)
            .unwrap();
        assert_eq!(
            rebalance_protocols.value.as_ref().unwrap().as_str(),
            "classic,consumer,streams"
        );
        assert!(rebalance_protocols.read_only);
        assert_eq!(rebalance_protocols.config_source, 4);
        assert_eq!(rebalance_protocols.synonyms.len(), 1);
        assert_eq!(rebalance_protocols.synonyms[0].source, 4);
        let share_assignors = broker_result
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == GROUP_SHARE_ASSIGNORS)
            .unwrap();
        let regex_refresh = broker_result
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == GROUP_CONSUMER_REGEX_REFRESH_INTERVAL_MS)
            .unwrap();
        for (name, expected) in [
            ("group.share.delivery.count.limit", "5"),
            ("group.share.min.delivery.count.limit", "2"),
            ("group.share.max.delivery.count.limit", "10"),
            ("group.share.partition.max.record.locks", "2000"),
            ("group.share.min.partition.max.record.locks", "100"),
            ("group.share.max.partition.max.record.locks", "4000"),
        ] {
            assert_eq!(
                broker_result
                    .configs
                    .iter()
                    .find(|entry| entry.name.as_str() == name)
                    .unwrap()
                    .value
                    .as_ref()
                    .unwrap()
                    .as_str(),
                expected
            );
        }
        assert_eq!(response.results[1].configs.len(), 9);
        for entry in response.results[1]
            .configs
            .iter()
            .filter(|entry| entry.name.as_str().ends_with("assignment.interval.ms"))
        {
            assert!(!entry.read_only);
            assert_eq!(entry.config_source, 4);
            assert_eq!(entry.value.as_ref().unwrap().as_str(), "1000");
        }
        for entry in response.results[1]
            .configs
            .iter()
            .filter(|entry| entry.name.as_str().ends_with("assignor.offload.enable"))
        {
            assert!(!entry.read_only);
            assert_eq!(entry.config_source, 4);
            assert_eq!(entry.value.as_ref().unwrap().as_str(), "true");
        }
        for name in [
            "group.coordinator.cached.buffer.max.bytes",
            "share.coordinator.cached.buffer.max.bytes",
        ] {
            let entry = response.results[1]
                .configs
                .iter()
                .find(|entry| entry.name.as_str() == name)
                .unwrap();
            assert!(!entry.read_only);
            assert_eq!(entry.config_source, 4);
            assert_eq!(entry.value.as_ref().unwrap().as_str(), "1048588");
        }

        let logger = &response.results[2].configs[0];
        assert_eq!(response.results[2].error_code, NO_ERROR);
        assert_eq!(logger.name.as_str(), "rutomq.tracing.filter");
        assert_eq!(
            logger.value.as_ref().unwrap().as_str(),
            "rutomq=debug,tower_http=warn"
        );
        assert!(logger.read_only);
        assert_eq!(logger.config_source, 6);
        assert_eq!(logger.synonyms[0].source, 6);
        if version >= 3 {
            assert_eq!(node_id.config_type, 3);
            assert_eq!(transactional_id_expiration.config_type, 3);
            assert!(
                transactional_id_expiration
                    .documentation
                    .as_ref()
                    .is_some_and(|value| !value.as_str().is_empty())
            );
            assert_eq!(transactional_id_cleanup.config_type, 3);
            assert!(
                transactional_id_cleanup
                    .documentation
                    .as_ref()
                    .is_some_and(|value| !value.as_str().is_empty())
            );
            assert_eq!(transaction_abort_cleanup.config_type, 3);
            assert!(
                transaction_abort_cleanup
                    .documentation
                    .as_ref()
                    .is_some_and(|value| !value.as_str().is_empty())
            );
            for (_, _, entry) in transaction_retry_backoffs {
                assert_eq!(entry.config_type, 3);
                assert!(
                    entry
                        .documentation
                        .as_ref()
                        .is_some_and(|value| !value.as_str().is_empty())
                );
            }
            assert_eq!(transaction_partition_verification.config_type, 1);
            assert!(
                transaction_partition_verification
                    .documentation
                    .as_ref()
                    .is_some_and(|value| !value.as_str().is_empty())
            );
            assert_eq!(transaction_max_timeout.config_type, 3);
            assert_eq!(max_reauth.config_type, 5);
            assert!(
                transaction_max_timeout
                    .documentation
                    .as_ref()
                    .is_some_and(|value| !value.as_str().is_empty())
            );
            assert_eq!(two_phase_commit.config_type, 1);
            assert!(
                two_phase_commit
                    .documentation
                    .as_ref()
                    .is_some_and(|value| !value.as_str().is_empty())
            );
            assert_eq!(rebalance_protocols.config_type, 7);
            assert_eq!(share_assignors.config_type, 7);
            assert_eq!(regex_refresh.config_type, 3);
            assert!(
                rebalance_protocols
                    .documentation
                    .as_ref()
                    .is_some_and(|value| !value.as_str().is_empty())
            );
            assert!(
                node_id
                    .documentation
                    .as_ref()
                    .is_some_and(|value| !value.as_str().is_empty())
            );
            assert_eq!(logger.config_type, 2);
            assert!(
                logger
                    .documentation
                    .as_ref()
                    .is_some_and(|value| !value.as_str().is_empty())
            );
        } else {
            assert_eq!(node_id.config_type, 0);
            assert_eq!(transactional_id_expiration.config_type, 0);
            assert_eq!(transactional_id_cleanup.config_type, 0);
            assert_eq!(transaction_abort_cleanup.config_type, 0);
            for (_, _, entry) in transaction_retry_backoffs {
                assert_eq!(entry.config_type, 0);
            }
            assert_eq!(transaction_partition_verification.config_type, 0);
            assert_eq!(transaction_max_timeout.config_type, 0);
            assert_eq!(max_reauth.config_type, 0);
            assert_eq!(two_phase_commit.config_type, 0);
            assert_eq!(rebalance_protocols.config_type, 0);
            assert_eq!(share_assignors.config_type, 0);
            assert_eq!(regex_refresh.config_type, 0);
            assert_eq!(logger.config_type, 0);
        }
    }
}

#[tokio::test]
async fn broker_config_description_validates_identity_and_cluster_acl() {
    let broker = configured_broker();
    let invalid = DescribeConfigsRequest::default().with_resources(vec![
        describe_resource(4, "1", None),
        describe_resource(8, "", None),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeConfigs, 4, 560, &invalid))
        .await
        .unwrap();
    let response: DescribeConfigsResponse = decode_response(ApiKey::DescribeConfigs, 4, response);
    assert!(
        response
            .results
            .iter()
            .all(|result| result.error_code == INVALID_REQUEST)
    );

    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    let broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    let denied = DescribeConfigsRequest::default().with_resources(vec![
        describe_resource(4, "0", None),
        describe_resource(8, "0", None),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeConfigs, 4, 561, &denied))
        .await
        .unwrap();
    let response: DescribeConfigsResponse = decode_response(ApiKey::DescribeConfigs, 4, response);
    assert!(
        response
            .results
            .iter()
            .all(|result| result.error_code == CLUSTER_AUTHORIZATION_FAILED)
    );
}
