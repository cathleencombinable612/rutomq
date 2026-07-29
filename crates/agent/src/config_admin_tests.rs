use super::acl_tests::{acl_broker, decode_response as decode_acl_response, handle_as, topic_rule};
use super::tests::{broker, decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    INVALID_CONFIG, INVALID_REQUEST, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR,
};
use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource as IncrementalResource, AlterableConfig as IncrementalConfig,
};
use kafka_protocol::messages::{
    AlterConfigsResponse, DescribeConfigsResponse, IncrementalAlterConfigsResponse,
};
use rutomq_control::{AclOperation, AclPermission, AclResourceType};
use std::collections::BTreeMap;

fn config(name: &str, value: Option<&str>) -> AlterableConfig {
    AlterableConfig::default()
        .with_name(StrBytes::from_string(name.to_owned()))
        .with_value(value.map(|value| StrBytes::from_string(value.to_owned())))
}

fn resource(name: &str, configs: Vec<AlterableConfig>) -> AlterConfigsResource {
    AlterConfigsResource::default()
        .with_resource_type(2)
        .with_resource_name(StrBytes::from_string(name.to_owned()))
        .with_configs(configs)
}

fn create_config(name: &str, value: &str) -> CreatableTopicConfig {
    CreatableTopicConfig::default()
        .with_name(StrBytes::from_string(name.to_owned()))
        .with_value(Some(StrBytes::from_string(value.to_owned())))
}

#[tokio::test]
async fn create_topics_applies_configs_atomically_and_honors_validate_only() {
    let broker = broker();
    let request = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("created-compact"))
            .with_num_partitions(1)
            .with_replication_factor(1)
            .with_configs(vec![
                create_config("cleanup.policy", "compact"),
                create_config("file.delete.delay.ms", "123"),
                create_config("flush.messages", "7"),
                create_config("flush.ms", "11"),
                create_config("delete.retention.ms", "500"),
                create_config("min.compaction.lag.ms", "100"),
                create_config("max.compaction.lag.ms", "1000"),
                create_config("min.cleanable.dirty.ratio", "0.75"),
                create_config("min.insync.replicas", "2"),
                create_config("max.message.bytes", "4096"),
                create_config("compression.type", "zstd"),
                create_config("compression.gzip.level", "9"),
                create_config("compression.lz4.level", "17"),
                create_config("compression.zstd.level", "22"),
                create_config("message.timestamp.type", "LogAppendTime"),
                create_config("message.timestamp.before.max.ms", "100"),
                create_config("message.timestamp.after.max.ms", "200"),
            ]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 59, &request))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(response.topics[0].error_code, NO_ERROR);
    let stored = broker
        .metadata
        .topic_config("created-compact")
        .await
        .unwrap();
    assert_eq!(stored.cleanup_policy, "compact");
    assert_eq!(stored.file_delete_delay_ms, 123);
    assert_eq!(stored.flush_messages, 7);
    assert_eq!(stored.flush_ms, 11);
    assert_eq!(stored.delete_retention_ms, 500);
    assert_eq!(stored.min_compaction_lag_ms, 100);
    assert_eq!(stored.max_compaction_lag_ms, 1000);
    assert_eq!(stored.min_cleanable_dirty_ratio, 0.75);
    assert_eq!(stored.min_insync_replicas, 2);
    assert_eq!(stored.max_message_bytes, 4096);
    assert_eq!(stored.compression_type, "zstd");
    assert_eq!(stored.compression_gzip_level, 9);
    assert_eq!(stored.compression_lz4_level, 17);
    assert_eq!(stored.compression_zstd_level, 22);
    assert_eq!(stored.message_timestamp_type, "LogAppendTime");
    assert_eq!(stored.message_timestamp_before_max_ms, 100);
    assert_eq!(stored.message_timestamp_after_max_ms, 200);
    assert!(stored.is_dynamic("cleanup.policy"));
    assert!(stored.is_dynamic("flush.messages"));
    assert!(!stored.is_dynamic("retention.ms"));

    let invalid = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("invalid-create-config"))
            .with_num_partitions(1)
            .with_configs(vec![
                create_config("cleanup.policy", "compact"),
                create_config("cleanup.policy", "delete"),
            ]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 60, &invalid))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(response.topics[0].error_code, INVALID_REQUEST);
    assert!(
        broker
            .metadata
            .topic("invalid-create-config")
            .await
            .unwrap()
            .is_none()
    );

    let invalid_timestamp = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("invalid-timestamp-config"))
            .with_num_partitions(1)
            .with_configs(vec![create_config("message.timestamp.type", "BrokerTime")]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::CreateTopics,
            7,
            62,
            &invalid_timestamp,
        ))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(response.topics[0].error_code, INVALID_CONFIG);

    let invalid_file_delete_delay = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("invalid-file-delete-delay"))
            .with_num_partitions(1)
            .with_configs(vec![create_config("file.delete.delay.ms", "-1")]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::CreateTopics,
            7,
            67,
            &invalid_file_delete_delay,
        ))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(response.topics[0].error_code, INVALID_CONFIG);

    for (correlation, name, value) in [(68, "flush.messages", "0"), (69, "flush.ms", "-1")] {
        let invalid_flush = CreateTopicsRequest::default().with_topics(vec![
            CreatableTopic::default()
                .with_name(topic_name(&format!("invalid-{name}")))
                .with_num_partitions(1)
                .with_configs(vec![create_config(name, value)]),
        ]);
        let response = broker
            .handle_request(request_frame(
                ApiKey::CreateTopics,
                7,
                correlation,
                &invalid_flush,
            ))
            .await
            .unwrap();
        let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
        assert_eq!(response.topics[0].error_code, INVALID_CONFIG);
    }

    let invalid_compression_level = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("invalid-compression-level"))
            .with_num_partitions(1)
            .with_configs(vec![create_config("compression.zstd.level", "23")]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::CreateTopics,
            7,
            66,
            &invalid_compression_level,
        ))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(response.topics[0].error_code, INVALID_CONFIG);

    let invalid_min_isr = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("invalid-min-isr-config"))
            .with_num_partitions(1)
            .with_configs(vec![create_config("min.insync.replicas", "0")]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 65, &invalid_min_isr))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(response.topics[0].error_code, INVALID_CONFIG);

    let invalid_compaction = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("invalid-compaction-config"))
            .with_num_partitions(1)
            .with_configs(vec![
                create_config("min.compaction.lag.ms", "1000"),
                create_config("max.compaction.lag.ms", "999"),
                create_config("min.cleanable.dirty.ratio", "1.1"),
            ]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::CreateTopics,
            7,
            64,
            &invalid_compaction,
        ))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(response.topics[0].error_code, INVALID_CONFIG);

    let invalid_compression = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("invalid-compression-config"))
            .with_num_partitions(1)
            .with_configs(vec![create_config("compression.type", "brotli")]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::CreateTopics,
            7,
            63,
            &invalid_compression,
        ))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(response.topics[0].error_code, INVALID_CONFIG);

    let validate = CreateTopicsRequest::default()
        .with_validate_only(true)
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(topic_name("validated-create-config"))
                .with_num_partitions(1)
                .with_replication_factor(1)
                .with_configs(vec![create_config("cleanup.policy", "compact")]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 61, &validate))
        .await
        .unwrap();
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    assert_eq!(response.topics[0].error_code, NO_ERROR);
    assert!(
        broker
            .metadata
            .topic("validated-create-config")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn legacy_alter_configs_replaces_topic_config_and_supports_validation() {
    let broker = broker();
    broker
        .metadata
        .create_topic("legacy-config", 1)
        .await
        .unwrap();
    broker
        .metadata
        .set_topic_config(
            "legacy-config",
            TopicConfig {
                retention_bytes: 99,
                cleanup_policy: "compact".to_owned(),
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();

    let replace = AlterConfigsRequest::default().with_resources(vec![resource(
        "legacy-config",
        vec![config("retention.ms", Some("1234"))],
    )]);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterConfigs, 2, 60, &replace))
        .await
        .unwrap();
    let response: AlterConfigsResponse = decode_response(ApiKey::AlterConfigs, 2, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    let stored = broker.metadata.topic_config("legacy-config").await.unwrap();
    assert_eq!(stored.retention_ms, 1234);
    assert_eq!(stored.retention_bytes, -1);
    assert_eq!(stored.cleanup_policy, "delete");
    assert!(stored.is_dynamic("retention.ms"));
    assert!(!stored.is_dynamic("cleanup.policy"));

    let validate = AlterConfigsRequest::default()
        .with_validate_only(true)
        .with_resources(vec![resource(
            "legacy-config",
            vec![config("retention.ms", Some("1"))],
        )]);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterConfigs, 2, 61, &validate))
        .await
        .unwrap();
    let response: AlterConfigsResponse = decode_response(ApiKey::AlterConfigs, 2, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    assert_eq!(
        broker
            .metadata
            .topic_config("legacy-config")
            .await
            .unwrap()
            .retention_ms,
        1234
    );
}

#[tokio::test]
async fn topic_config_sources_follow_incremental_set_and_delete() {
    let broker = broker();
    let create = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("source-transitions"))
            .with_num_partitions(1)
            .with_replication_factor(1)
            .with_configs(vec![create_config("retention.ms", "123")]),
    ]);
    broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 70, &create))
        .await
        .unwrap();
    assert_eq!(
        described_topic_source(&broker, "source-transitions", "retention.ms").await,
        1
    );
    assert_eq!(
        described_topic_source(&broker, "source-transitions", "cleanup.policy").await,
        5
    );

    let alter = IncrementalAlterConfigsRequest::default().with_resources(vec![
        IncrementalResource::default()
            .with_resource_type(2)
            .with_resource_name(StrBytes::from_string("source-transitions".to_owned()))
            .with_configs(vec![
                IncrementalConfig::default()
                    .with_name(StrBytes::from_string("retention.ms".to_owned()))
                    .with_value(None)
                    .with_config_operation(1),
                IncrementalConfig::default()
                    .with_name(StrBytes::from_string("cleanup.policy".to_owned()))
                    .with_value(Some(StrBytes::from_string("compact".to_owned())))
                    .with_config_operation(0),
            ]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            71,
            &alter,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    assert_eq!(
        described_topic_source(&broker, "source-transitions", "retention.ms").await,
        5
    );
    assert_eq!(
        described_topic_source(&broker, "source-transitions", "cleanup.policy").await,
        1
    );
}

#[tokio::test]
async fn topic_config_synonyms_follow_kafka_precedence_in_versions_one_through_four() {
    let broker = broker();
    let create = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("config-synonyms"))
            .with_num_partitions(1)
            .with_replication_factor(1)
            .with_configs(vec![create_config("retention.ms", "123")]),
    ]);
    broker
        .handle_request(request_frame(ApiKey::CreateTopics, 7, 73, &create))
        .await
        .unwrap();

    for version in 1..=4 {
        for include_synonyms in [false, true] {
            let request = DescribeConfigsRequest::default()
                .with_include_synonyms(include_synonyms)
                .with_resources(vec![
                    DescribeConfigsResource::default()
                        .with_resource_type(2)
                        .with_resource_name(StrBytes::from_static_str("config-synonyms"))
                        .with_configuration_keys(Some(vec![
                            StrBytes::from_static_str("retention.ms"),
                            StrBytes::from_static_str("cleanup.policy"),
                        ])),
                ]);
            let response = broker
                .handle_request(request_frame(
                    ApiKey::DescribeConfigs,
                    version,
                    74 + i32::from(version),
                    &request,
                ))
                .await
                .unwrap();
            let response: DescribeConfigsResponse =
                decode_response(ApiKey::DescribeConfigs, version, response);
            assert_eq!(response.results[0].error_code, NO_ERROR);
            let configs = &response.results[0].configs;
            let retention = configs
                .iter()
                .find(|config| config.name.as_str() == "retention.ms")
                .unwrap();
            let cleanup = configs
                .iter()
                .find(|config| config.name.as_str() == "cleanup.policy")
                .unwrap();
            if !include_synonyms {
                assert!(retention.synonyms.is_empty());
                assert!(cleanup.synonyms.is_empty());
                continue;
            }
            assert_eq!(retention.synonyms.len(), 2);
            assert_synonym(&retention.synonyms[0], "retention.ms", "123", 1);
            assert_synonym(
                &retention.synonyms[1],
                "log.retention.ms",
                &TopicConfig::default().retention_ms.to_string(),
                5,
            );
            assert_eq!(cleanup.synonyms.len(), 1);
            assert_synonym(&cleanup.synonyms[0], "log.cleanup.policy", "delete", 5);
        }
    }
}

fn assert_synonym(
    synonym: &kafka_protocol::messages::describe_configs_response::DescribeConfigsSynonym,
    name: &str,
    value: &str,
    source: i8,
) {
    assert_eq!(synonym.name.as_str(), name);
    assert_eq!(synonym.value.as_ref().unwrap().as_str(), value);
    assert_eq!(synonym.source, source);
}

async fn described_topic_source(broker: &Broker, topic: &str, key: &str) -> i8 {
    let request = DescribeConfigsRequest::default().with_resources(vec![
        DescribeConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(StrBytes::from_string(topic.to_owned()))
            .with_configuration_keys(Some(vec![StrBytes::from_string(key.to_owned())])),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeConfigs, 4, 72, &request))
        .await
        .unwrap();
    let response: DescribeConfigsResponse = decode_response(ApiKey::DescribeConfigs, 4, response);
    response.results[0].configs[0].config_source
}

#[tokio::test]
async fn alter_configs_rejects_duplicates_and_null_values_without_mutation() {
    let broker = broker();
    broker
        .metadata
        .create_topic("invalid-config", 1)
        .await
        .unwrap();
    let duplicate = resource(
        "invalid-config",
        vec![
            config("retention.ms", Some("1")),
            config("retention.ms", Some("2")),
        ],
    );
    let request = AlterConfigsRequest::default().with_resources(vec![duplicate]);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterConfigs, 2, 62, &request))
        .await
        .unwrap();
    let response: AlterConfigsResponse = decode_response(ApiKey::AlterConfigs, 2, response);
    assert_eq!(response.responses[0].error_code, INVALID_REQUEST);

    let duplicate_resource = resource("invalid-config", vec![config("retention.ms", Some("3"))]);
    let request = AlterConfigsRequest::default()
        .with_resources(vec![duplicate_resource.clone(), duplicate_resource]);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterConfigs, 2, 63, &request))
        .await
        .unwrap();
    let response: AlterConfigsResponse = decode_response(ApiKey::AlterConfigs, 2, response);
    assert!(
        response
            .responses
            .iter()
            .all(|resource| resource.error_code == INVALID_REQUEST)
    );

    let request = AlterConfigsRequest::default().with_resources(vec![resource(
        "invalid-config",
        vec![config("retention.ms", None)],
    )]);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterConfigs, 2, 64, &request))
        .await
        .unwrap();
    let response: AlterConfigsResponse = decode_response(ApiKey::AlterConfigs, 2, response);
    assert_eq!(response.responses[0].error_code, INVALID_REQUEST);
    assert_eq!(
        broker
            .metadata
            .topic_config("invalid-config")
            .await
            .unwrap(),
        TopicConfig::default()
    );
}

#[tokio::test]
async fn group_configs_round_trip_with_dynamic_source_and_validation() {
    let broker = broker();
    let change = |name: &str, value: &str| {
        IncrementalConfig::default()
            .with_name(StrBytes::from_string(name.to_owned()))
            .with_value(Some(StrBytes::from_string(value.to_owned())))
            .with_config_operation(0)
    };
    let request = IncrementalAlterConfigsRequest::default()
        .with_validate_only(true)
        .with_resources(vec![
            IncrementalResource::default()
                .with_resource_type(32)
                .with_resource_name(StrBytes::from_string("streams-config-app".to_owned()))
                .with_configs(vec![
                    change("consumer.assignment.interval.ms", "125"),
                    change("consumer.assignor.offload.enable", "false"),
                    change("consumer.heartbeat.interval.ms", "15000"),
                    change("consumer.session.timeout.ms", "60000"),
                    change("share.assignment.interval.ms", "250"),
                    change("share.assignor.offload.enable", "false"),
                    change("share.heartbeat.interval.ms", "15000"),
                    change("share.session.timeout.ms", "60000"),
                    change("streams.assignment.interval.ms", "500"),
                    change("streams.assignor.offload.enable", "false"),
                    change("streams.heartbeat.interval.ms", "15000"),
                    change("streams.session.timeout.ms", "60000"),
                    change("streams.num.standby.replicas", "1"),
                    change("streams.initial.rebalance.delay.ms", "25"),
                    change("share.auto.offset.reset", "by_duration:PT30S"),
                    change("share.delivery.count.limit", "3"),
                    change("share.partition.max.record.locks", "123"),
                    change("share.record.lock.duration.ms", "60000"),
                    change("share.renew.acknowledge.enable", "false"),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            65,
            &request,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    assert!(
        broker
            .metadata
            .group_config("streams-config-app")
            .await
            .unwrap()
            .is_empty()
    );

    let committed = request.with_validate_only(false);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            66,
            &committed,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    let runtime = broker
        .group_runtime_config("streams-config-app")
        .await
        .unwrap();
    assert_eq!(runtime.consumer_assignment_interval_ms, 125);
    assert!(!runtime.consumer_assignor_offload_enable);
    assert_eq!(runtime.consumer_heartbeat_interval_ms, 15_000);
    assert_eq!(runtime.consumer_session_timeout_ms, 60_000);
    assert_eq!(runtime.share_assignment_interval_ms, 250);
    assert!(!runtime.share_assignor_offload_enable);
    assert_eq!(runtime.share_heartbeat_interval_ms, 15_000);
    assert_eq!(runtime.share_session_timeout_ms, 60_000);
    assert_eq!(runtime.streams_assignment_interval_ms, 500);
    assert!(!runtime.streams_assignor_offload_enable);
    assert_eq!(runtime.streams_heartbeat_interval_ms, 15_000);
    assert_eq!(runtime.streams_session_timeout_ms, 60_000);
    assert_eq!(runtime.streams_num_standby_replicas, 1);
    assert_eq!(runtime.streams_initial_rebalance_delay_ms, 25);
    assert_eq!(
        runtime.share_auto_offset_reset.configured_value(),
        "by_duration:PT30S"
    );
    assert_eq!(runtime.share_delivery_count_limit, 3);
    assert_eq!(runtime.share_partition_max_record_locks, 123);
    assert_eq!(runtime.share_record_lock_duration_ms, 60_000);
    assert!(!runtime.share_renew_acknowledge_enable);

    let describe = DescribeConfigsRequest::default()
        .with_include_synonyms(true)
        .with_include_documentation(true)
        .with_resources(vec![
            DescribeConfigsResource::default()
                .with_resource_type(32)
                .with_resource_name(StrBytes::from_string("streams-config-app".to_owned()))
                .with_configuration_keys(None),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeConfigs, 4, 67, &describe))
        .await
        .unwrap();
    let response: DescribeConfigsResponse = decode_response(ApiKey::DescribeConfigs, 4, response);
    assert_eq!(response.results[0].error_code, NO_ERROR);
    assert_eq!(response.results[0].configs.len(), 20);
    let standby = response.results[0]
        .configs
        .iter()
        .find(|config| config.name.as_str() == "streams.num.standby.replicas")
        .unwrap();
    assert_eq!(standby.value.as_ref().unwrap().as_str(), "1");
    assert_eq!(standby.config_source, 8);
    assert_eq!(standby.config_type, 3);
    assert!(standby.documentation.is_some());
    assert_eq!(standby.synonyms.len(), 1);
    assert_synonym(&standby.synonyms[0], "streams.num.standby.replicas", "1", 8);
    let renew = response.results[0]
        .configs
        .iter()
        .find(|config| config.name.as_str() == "share.renew.acknowledge.enable")
        .unwrap();
    assert_eq!(renew.value.as_ref().unwrap().as_str(), "false");
    assert_eq!(renew.config_source, 8);
    assert_eq!(renew.config_type, 1);
    let default_entry = response.results[0]
        .configs
        .iter()
        .find(|config| config.name.as_str() == "share.isolation.level")
        .unwrap();
    assert_eq!(default_entry.config_source, 5);
    assert_eq!(default_entry.synonyms.len(), 1);
    assert_synonym(
        &default_entry.synonyms[0],
        "share.isolation.level",
        default_entry.value.as_ref().unwrap().as_str(),
        5,
    );

    let without_synonyms = describe.with_include_synonyms(false);
    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeConfigs,
            4,
            70,
            &without_synonyms,
        ))
        .await
        .unwrap();
    let response: DescribeConfigsResponse = decode_response(ApiKey::DescribeConfigs, 4, response);
    assert!(
        response.results[0]
            .configs
            .iter()
            .all(|config| config.synonyms.is_empty())
    );

    let invalid = IncrementalAlterConfigsRequest::default().with_resources(vec![
        IncrementalResource::default()
            .with_resource_type(32)
            .with_resource_name(StrBytes::from_string("streams-config-app".to_owned()))
            .with_configs(vec![change("streams.num.standby.replicas", "-1")]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            68,
            &invalid,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, INVALID_REQUEST);
    assert_eq!(
        broker
            .group_runtime_config("streams-config-app")
            .await
            .unwrap()
            .streams_num_standby_replicas,
        1
    );

    for (correlation, name, value) in [
        (71, "share.delivery.count.limit", "1"),
        (72, "share.partition.max.record.locks", "99"),
        (73, "share.renew.acknowledge.enable", "sometimes"),
        (74, "consumer.assignment.interval.ms", "-1"),
        (75, "share.assignment.interval.ms", "15001"),
        (76, "streams.assignment.interval.ms", "not-an-integer"),
        (761, "consumer.heartbeat.interval.ms", "4999"),
        (762, "consumer.heartbeat.interval.ms", "15001"),
        (763, "consumer.session.timeout.ms", "44999"),
        (764, "consumer.session.timeout.ms", "60001"),
        (765, "share.heartbeat.interval.ms", "4999"),
        (766, "share.heartbeat.interval.ms", "15001"),
        (767, "share.session.timeout.ms", "44999"),
        (768, "share.session.timeout.ms", "60001"),
        (769, "streams.heartbeat.interval.ms", "4999"),
        (770, "streams.heartbeat.interval.ms", "15001"),
        (771, "streams.session.timeout.ms", "44999"),
        (772, "streams.session.timeout.ms", "60001"),
        (773, "streams.num.standby.replicas", "3"),
        (774, "share.record.lock.duration.ms", "14999"),
        (775, "share.record.lock.duration.ms", "60001"),
    ] {
        let invalid = IncrementalAlterConfigsRequest::default().with_resources(vec![
            IncrementalResource::default()
                .with_resource_type(32)
                .with_resource_name(StrBytes::from_string("streams-config-app".to_owned()))
                .with_configs(vec![change(name, value)]),
        ]);
        let response = broker
            .handle_request(request_frame(
                ApiKey::IncrementalAlterConfigs,
                1,
                correlation,
                &invalid,
            ))
            .await
            .unwrap();
        let response: IncrementalAlterConfigsResponse =
            decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
        assert_eq!(response.responses[0].error_code, INVALID_REQUEST);
    }
    let unchanged = broker
        .group_runtime_config("streams-config-app")
        .await
        .unwrap();
    assert_eq!(unchanged.consumer_heartbeat_interval_ms, 15_000);
    assert_eq!(unchanged.consumer_session_timeout_ms, 60_000);
    assert_eq!(unchanged.share_heartbeat_interval_ms, 15_000);
    assert_eq!(unchanged.share_session_timeout_ms, 60_000);
    assert_eq!(unchanged.streams_heartbeat_interval_ms, 15_000);
    assert_eq!(unchanged.streams_session_timeout_ms, 60_000);
    assert_eq!(unchanged.streams_num_standby_replicas, 1);
    assert_eq!(unchanged.share_record_lock_duration_ms, 60_000);

    broker
        .metadata
        .alter_group_config(
            "persisted-outside-new-bounds",
            BTreeMap::from([
                (
                    "share.delivery.count.limit".to_owned(),
                    Some("25".to_owned()),
                ),
                (
                    "share.partition.max.record.locks".to_owned(),
                    Some("9999".to_owned()),
                ),
                (
                    "consumer.assignment.interval.ms".to_owned(),
                    Some("20000".to_owned()),
                ),
                (
                    "consumer.heartbeat.interval.ms".to_owned(),
                    Some("20000".to_owned()),
                ),
                (
                    "consumer.session.timeout.ms".to_owned(),
                    Some("70000".to_owned()),
                ),
                (
                    "share.assignment.interval.ms".to_owned(),
                    Some("20000".to_owned()),
                ),
                (
                    "share.heartbeat.interval.ms".to_owned(),
                    Some("20000".to_owned()),
                ),
                (
                    "share.session.timeout.ms".to_owned(),
                    Some("70000".to_owned()),
                ),
                (
                    "streams.assignment.interval.ms".to_owned(),
                    Some("20000".to_owned()),
                ),
                (
                    "streams.heartbeat.interval.ms".to_owned(),
                    Some("20000".to_owned()),
                ),
                (
                    "streams.session.timeout.ms".to_owned(),
                    Some("70000".to_owned()),
                ),
                (
                    "streams.num.standby.replicas".to_owned(),
                    Some("99".to_owned()),
                ),
                (
                    "share.record.lock.duration.ms".to_owned(),
                    Some("70000".to_owned()),
                ),
            ]),
            false,
        )
        .await
        .unwrap();
    let capped = broker
        .group_runtime_config("persisted-outside-new-bounds")
        .await
        .unwrap();
    assert_eq!(capped.share_delivery_count_limit, 10);
    assert_eq!(capped.share_partition_max_record_locks, 4_000);
    assert_eq!(capped.consumer_assignment_interval_ms, 15_000);
    assert_eq!(capped.consumer_heartbeat_interval_ms, 15_000);
    assert_eq!(capped.consumer_session_timeout_ms, 60_000);
    assert_eq!(capped.share_assignment_interval_ms, 15_000);
    assert_eq!(capped.share_heartbeat_interval_ms, 15_000);
    assert_eq!(capped.share_session_timeout_ms, 60_000);
    assert_eq!(capped.streams_assignment_interval_ms, 15_000);
    assert_eq!(capped.streams_heartbeat_interval_ms, 15_000);
    assert_eq!(capped.streams_session_timeout_ms, 60_000);
    assert_eq!(capped.streams_num_standby_replicas, 2);
    assert_eq!(capped.share_record_lock_duration_ms, 60_000);

    let invalid_duration = IncrementalAlterConfigsRequest::default().with_resources(vec![
        IncrementalResource::default()
            .with_resource_type(32)
            .with_resource_name(StrBytes::from_string("streams-config-app".to_owned()))
            .with_configs(vec![change("share.auto.offset.reset", "by_duration:-PT1S")]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            69,
            &invalid_duration,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, INVALID_REQUEST);
}

#[tokio::test]
async fn group_broker_defaults_are_cluster_wide_and_dynamic() {
    let broker = broker();
    let change = |name: &str, value: Option<&str>, operation: i8| {
        IncrementalConfig::default()
            .with_name(StrBytes::from_string(name.to_owned()))
            .with_value(value.map(|value| StrBytes::from_string(value.to_owned())))
            .with_config_operation(operation)
    };
    let resource = IncrementalResource::default()
        .with_resource_type(4)
        .with_resource_name(StrBytes::default())
        .with_configs(vec![
            change("group.consumer.assignment.interval.ms", Some("200"), 0),
            change("group.share.assignment.interval.ms", Some("300"), 0),
            change("group.streams.assignment.interval.ms", Some("400"), 0),
            change("group.consumer.assignor.offload.enable", Some("false"), 0),
            change("group.share.assignor.offload.enable", Some("false"), 0),
            change("group.streams.assignor.offload.enable", Some("true"), 0),
            change(
                "group.coordinator.cached.buffer.max.bytes",
                Some("524288"),
                0,
            ),
            change(
                "share.coordinator.cached.buffer.max.bytes",
                Some("2097152"),
                0,
            ),
            change(
                "transaction.partition.verification.enable",
                Some("false"),
                0,
            ),
        ]);
    let validate = IncrementalAlterConfigsRequest::default()
        .with_validate_only(true)
        .with_resources(vec![resource.clone()]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            77,
            &validate,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    assert!(broker.metadata.broker_config().await.unwrap().is_empty());

    let commit = IncrementalAlterConfigsRequest::default().with_resources(vec![resource]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            78,
            &commit,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    let runtime = broker
        .group_runtime_config("inherits-defaults")
        .await
        .unwrap();
    assert_eq!(runtime.consumer_assignment_interval_ms, 200);
    assert_eq!(runtime.share_assignment_interval_ms, 300);
    assert_eq!(runtime.streams_assignment_interval_ms, 400);
    assert!(!runtime.consumer_assignor_offload_enable);
    assert!(!runtime.share_assignor_offload_enable);
    assert!(runtime.streams_assignor_offload_enable);
    assert!(
        !broker
            .transaction_partition_verification_enabled()
            .await
            .unwrap()
    );
    broker
        .metadata
        .alter_group_config(
            "inherits-defaults",
            BTreeMap::from([(
                "consumer.assignment.interval.ms".to_owned(),
                Some("700".to_owned()),
            )]),
            false,
        )
        .await
        .unwrap();
    let overridden = broker
        .group_runtime_config("inherits-defaults")
        .await
        .unwrap();
    assert_eq!(overridden.consumer_assignment_interval_ms, 700);
    assert_eq!(overridden.share_assignment_interval_ms, 300);

    let describe = DescribeConfigsRequest::default()
        .with_include_synonyms(true)
        .with_resources(vec![
            DescribeConfigsResource::default()
                .with_resource_type(4)
                .with_resource_name(StrBytes::default())
                .with_configuration_keys(None),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeConfigs, 4, 79, &describe))
        .await
        .unwrap();
    let response: DescribeConfigsResponse = decode_response(ApiKey::DescribeConfigs, 4, response);
    assert_eq!(response.results[0].configs.len(), 9);
    for (name, value) in [
        ("group.consumer.assignment.interval.ms", "200"),
        ("group.share.assignment.interval.ms", "300"),
        ("group.streams.assignment.interval.ms", "400"),
    ] {
        let entry = response.results[0]
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == name)
            .unwrap();
        assert_eq!(entry.value.as_ref().unwrap().as_str(), value);
        assert_eq!(entry.config_source, 3);
        assert!(!entry.read_only);
        assert_eq!(entry.synonyms.len(), 2);
        assert_synonym(&entry.synonyms[0], name, value, 3);
        assert_synonym(&entry.synonyms[1], name, "0", 4);
    }
    for (name, value) in [
        ("group.consumer.assignor.offload.enable", "false"),
        ("group.share.assignor.offload.enable", "false"),
        ("group.streams.assignor.offload.enable", "true"),
        ("transaction.partition.verification.enable", "false"),
    ] {
        let entry = response.results[0]
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == name)
            .unwrap();
        assert_eq!(entry.value.as_ref().unwrap().as_str(), value);
        assert_eq!(entry.config_source, 3);
        assert!(!entry.read_only);
        assert_eq!(entry.synonyms.len(), 2);
    }
    for (name, value) in [
        ("group.coordinator.cached.buffer.max.bytes", "524288"),
        ("share.coordinator.cached.buffer.max.bytes", "2097152"),
    ] {
        let entry = response.results[0]
            .configs
            .iter()
            .find(|entry| entry.name.as_str() == name)
            .unwrap();
        assert_eq!(entry.value.as_ref().unwrap().as_str(), value);
        assert_eq!(entry.config_source, 3);
        assert_eq!(entry.config_type, 3);
        assert!(!entry.read_only);
        assert_eq!(entry.synonyms.len(), 2);
        assert_synonym(&entry.synonyms[0], name, value, 3);
        assert_synonym(&entry.synonyms[1], name, "1048588", 4);
    }

    let invalid = IncrementalAlterConfigsRequest::default().with_resources(vec![
        IncrementalResource::default()
            .with_resource_type(4)
            .with_resource_name(StrBytes::default())
            .with_configs(vec![change(
                "group.consumer.assignment.interval.ms",
                Some("15001"),
                0,
            )]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            80,
            &invalid,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, INVALID_REQUEST);

    let invalid_boolean = IncrementalAlterConfigsRequest::default().with_resources(vec![
        IncrementalResource::default()
            .with_resource_type(4)
            .with_resource_name(StrBytes::default())
            .with_configs(vec![change(
                "transaction.partition.verification.enable",
                Some("disabled"),
                0,
            )]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            801,
            &invalid_boolean,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, INVALID_REQUEST);

    let invalid_buffer = IncrementalAlterConfigsRequest::default().with_resources(vec![
        IncrementalResource::default()
            .with_resource_type(4)
            .with_resource_name(StrBytes::default())
            .with_configs(vec![change(
                "group.coordinator.cached.buffer.max.bytes",
                Some("524287"),
                0,
            )]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            81,
            &invalid_buffer,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, INVALID_REQUEST);

    let per_broker = IncrementalAlterConfigsRequest::default().with_resources(vec![
        IncrementalResource::default()
            .with_resource_type(4)
            .with_resource_name(StrBytes::from_static_str("0"))
            .with_configs(vec![change(
                "group.consumer.assignment.interval.ms",
                Some("500"),
                0,
            )]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            82,
            &per_broker,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, INVALID_REQUEST);

    let before = broker.metadata.broker_config().await.unwrap();
    for (correlation_id, name, value) in [
        (84, "group.coordinator.rebalance.protocols", "classic"),
        (85, "add.partitions.to.txn.retry.backoff.ms", "25"),
        (86, "add.partitions.to.txn.retry.backoff.max.ms", "125"),
    ] {
        let read_only = IncrementalAlterConfigsRequest::default().with_resources(vec![
            IncrementalResource::default()
                .with_resource_type(4)
                .with_resource_name(StrBytes::default())
                .with_configs(vec![change(name, Some(value), 0)]),
        ]);
        let response = broker
            .handle_request(request_frame(
                ApiKey::IncrementalAlterConfigs,
                1,
                correlation_id,
                &read_only,
            ))
            .await
            .unwrap();
        let response: IncrementalAlterConfigsResponse =
            decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
        assert_eq!(response.responses[0].error_code, INVALID_REQUEST);
        assert_eq!(broker.metadata.broker_config().await.unwrap(), before);
    }

    let legacy = AlterConfigsRequest::default().with_resources(vec![
        AlterConfigsResource::default()
            .with_resource_type(4)
            .with_resource_name(StrBytes::default())
            .with_configs(vec![config(
                "group.consumer.assignment.interval.ms",
                Some("600"),
            )]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::AlterConfigs, 2, 83, &legacy))
        .await
        .unwrap();
    let response: AlterConfigsResponse = decode_response(ApiKey::AlterConfigs, 2, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    let runtime = broker
        .group_runtime_config("legacy-defaults")
        .await
        .unwrap();
    assert_eq!(runtime.consumer_assignment_interval_ms, 600);
    assert_eq!(runtime.share_assignment_interval_ms, 0);
    assert_eq!(runtime.streams_assignment_interval_ms, 0);
    let persisted = broker.metadata.broker_config().await.unwrap();
    assert_eq!(
        persisted,
        BTreeMap::from([(
            "group.consumer.assignment.interval.ms".to_owned(),
            "600".to_owned(),
        )])
    );
}

#[tokio::test]
async fn alter_configs_backend_failure_prevents_legacy_and_incremental_mutation() {
    let (broker, metadata) = acl_broker();
    for topic in ["legacy-auth-atomic", "incremental-auth-atomic"] {
        metadata.create_topic(topic, 1).await.unwrap();
        metadata
            .create_acl(topic_rule(
                "User:config-writer",
                topic,
                AclOperation::AlterConfigs,
                AclPermission::Allow,
            ))
            .await
            .unwrap();
    }
    metadata.set_authorization_failure_for(Some(AclResourceType::Group));

    let legacy = AlterConfigsRequest::default().with_resources(vec![
        AlterConfigsResource::default()
            .with_resource_type(99)
            .with_resource_name(StrBytes::from_static_str("invalid-legacy-resource"))
            .with_configs(vec![config("retention.ms", Some("1"))]),
        resource(
            "legacy-auth-atomic",
            vec![config("retention.ms", Some("123"))],
        ),
        AlterConfigsResource::default()
            .with_resource_type(32)
            .with_resource_name(StrBytes::from_static_str("legacy-auth-group"))
            .with_configs(vec![config("streams.heartbeat.interval.ms", Some("5000"))]),
    ]);
    let response = handle_as(
        &broker,
        "config-writer",
        ApiKey::AlterConfigs,
        2,
        8601,
        &legacy,
    )
    .await;
    let response: AlterConfigsResponse = decode_acl_response(ApiKey::AlterConfigs, 2, response);
    assert_eq!(
        response
            .responses
            .iter()
            .map(|result| result.error_code)
            .collect::<Vec<_>>(),
        [INVALID_REQUEST, UNKNOWN_SERVER_ERROR, UNKNOWN_SERVER_ERROR]
    );
    assert_eq!(
        metadata.topic_config("legacy-auth-atomic").await.unwrap(),
        TopicConfig::default()
    );

    let incremental = IncrementalAlterConfigsRequest::default().with_resources(vec![
        IncrementalResource::default()
            .with_resource_type(99)
            .with_resource_name(StrBytes::from_static_str("invalid-incremental-resource"))
            .with_configs(vec![
                IncrementalConfig::default()
                    .with_name(StrBytes::from_static_str("retention.ms"))
                    .with_value(Some(StrBytes::from_static_str("1")))
                    .with_config_operation(0),
            ]),
        IncrementalResource::default()
            .with_resource_type(2)
            .with_resource_name(StrBytes::from_static_str("incremental-auth-atomic"))
            .with_configs(vec![
                IncrementalConfig::default()
                    .with_name(StrBytes::from_static_str("retention.ms"))
                    .with_value(Some(StrBytes::from_static_str("456")))
                    .with_config_operation(0),
            ]),
        IncrementalResource::default()
            .with_resource_type(32)
            .with_resource_name(StrBytes::from_static_str("incremental-auth-group"))
            .with_configs(vec![
                IncrementalConfig::default()
                    .with_name(StrBytes::from_static_str("streams.heartbeat.interval.ms"))
                    .with_value(Some(StrBytes::from_static_str("5000")))
                    .with_config_operation(0),
            ]),
    ]);
    let response = handle_as(
        &broker,
        "config-writer",
        ApiKey::IncrementalAlterConfigs,
        1,
        8602,
        &incremental,
    )
    .await;
    let response: IncrementalAlterConfigsResponse =
        decode_acl_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(
        response
            .responses
            .iter()
            .map(|result| result.error_code)
            .collect::<Vec<_>>(),
        [INVALID_REQUEST, UNKNOWN_SERVER_ERROR, UNKNOWN_SERVER_ERROR]
    );
    assert_eq!(
        metadata
            .topic_config("incremental-auth-atomic")
            .await
            .unwrap(),
        TopicConfig::default()
    );
    assert!(
        metadata
            .group_config("incremental-auth-group")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn incremental_alter_configs_preserves_preprocessing_and_partial_denial() {
    let (broker, metadata) = acl_broker();
    for topic in ["visible-config-write", "hidden-config-write"] {
        metadata.create_topic(topic, 1).await.unwrap();
    }
    metadata
        .create_acl(topic_rule(
            "User:config-writer",
            "visible-config-write",
            AclOperation::AlterConfigs,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    metadata.set_authorization_failure_for(Some(AclResourceType::Cluster));

    let change = |resource_type, name: &'static str, value: &'static str| {
        IncrementalResource::default()
            .with_resource_type(resource_type)
            .with_resource_name(StrBytes::from_static_str(name))
            .with_configs(vec![
                IncrementalConfig::default()
                    .with_name(StrBytes::from_static_str("retention.ms"))
                    .with_value(Some(StrBytes::from_static_str(value)))
                    .with_config_operation(0),
            ])
    };
    let request = IncrementalAlterConfigsRequest::default().with_resources(vec![
        change(99, "invalid-config-resource", "1"),
        change(2, "hidden-config-write", "2"),
        change(2, "visible-config-write", "3"),
    ]);
    let response = handle_as(
        &broker,
        "config-writer",
        ApiKey::IncrementalAlterConfigs,
        1,
        8603,
        &request,
    )
    .await;
    let response: IncrementalAlterConfigsResponse =
        decode_acl_response(ApiKey::IncrementalAlterConfigs, 1, response);

    assert_eq!(
        response
            .responses
            .iter()
            .map(|result| result.error_code)
            .collect::<Vec<_>>(),
        [INVALID_REQUEST, TOPIC_AUTHORIZATION_FAILED, NO_ERROR]
    );
    assert_eq!(
        metadata.topic_config("hidden-config-write").await.unwrap(),
        TopicConfig::default()
    );
    assert_eq!(
        metadata
            .topic_config("visible-config-write")
            .await
            .unwrap()
            .retention_ms,
        3
    );
}
