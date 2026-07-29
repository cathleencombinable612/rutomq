#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${script_dir}/../.." && pwd)"
compose_file="${script_dir}/docker-compose.yml"
compose_project="${RUTOMQ_FLINK_COMPOSE_PROJECT:-rutomq-flink-it}"
run_id="${RUTOMQ_FLINK_RUN_ID:-$(date +%s)}"
records="${RUTOMQ_FLINK_RECORDS:-1000}"
flink_version="${RUTOMQ_FLINK_VERSION:-2.2.1}"
connector_version="${RUTOMQ_FLINK_KAFKA_VERSION:-5.0.0-2.2}"
health_port="${RUTOMQ_FLINK_HEALTH_PORT:-18080}"
export RUTOMQ_FLINK_HEALTH_PORT="${health_port}"
export RUTOMQ_FLINK_KAFKA_PORT="${RUTOMQ_FLINK_KAFKA_PORT:-19092}"
export RUTOMQ_FLINK_UI_PORT="${RUTOMQ_FLINK_UI_PORT:-18081}"
export FLINK_IMAGE="${FLINK_IMAGE:-flink:${flink_version}-scala_2.12-java21}"
partitions=2
input_topic="flink-input-${run_id}"
output_topic="flink-output-${run_id}"
group_id="flink-group-${run_id}"
consumer_group_id="consumer-protocol-group-${run_id}"
consumer_topic="consumer-protocol-topic-${run_id}"
delete_groups_duplicate_topic="delete-groups-duplicate-topic-${run_id}"
delete_groups_duplicate_prefix="delete-groups-duplicate-${run_id}"
group_describe_duplicate_prefix="group-describe-duplicate-${run_id}"
static_group_id="classic-static-group-${run_id}"
classic_join_barrier_group_id="classic-join-barrier-${run_id}"
static_topic="classic-static-topic-${run_id}"
record_admission_topic="record-admission-topic-${run_id}"
produce_missing_topic_prefix="produce-missing-topic-${run_id}"
compression_topic_prefix="compression-topic-${run_id}"
share_group_id="share-group-${run_id}"
share_topic="share-topic-${run_id}"
share_duration_group_id="share-duration-group-${run_id}"
share_duration_topic="share-duration-topic-${run_id}"
quota_topic="client-quota-topic-${run_id}"
compaction_topic="compaction-topic-${run_id}"
min_isr_topic="min-isr-topic-${run_id}"
file_delete_delay_topic="file-delete-delay-topic-${run_id}"
flush_policy_topic="flush-policy-topic-${run_id}"
create_topics_assignment_topic_prefix="create-topics-assignment-${run_id}"
create_topics_identity_topic_prefix="create-topics-identity-${run_id}"
delete_topics_identity_topic_prefix="delete-topics-identity-${run_id}"
delete_topics_privacy_prefix="delete-topics-privacy-${run_id}"
create_acls_validation_prefix="create-acls-validation-${run_id}"
metadata_semantics_prefix="metadata-semantics-${run_id}"
group_heartbeat_recovery_prefix="group-heartbeat-recovery-${run_id}"
streams_topology_validation_prefix="streams-topology-validation-${run_id}"
create_partitions_duplicate_topic_prefix="create-partitions-duplicate-${run_id}"
describe_topic_partitions_prefix="describe-topic-partitions-${run_id}"
unsupported_topic_config_prefix="unsupported-topic-config-${run_id}"
legacy_config_prefix="legacy-non-topic-config-${run_id}"
list_offsets_topic="list-offsets-topic-${run_id}"
kip939_topic="kip939-topic-${run_id}"
two_phase_acl_prefix="two-phase-acl-${run_id}"
transaction_legacy_prefix="transaction-legacy-${run_id}"
transaction_query_prefix="transaction-query-${run_id}"
streams_input_topic="streams-input-${run_id}"
streams_output_topic="streams-output-${run_id}"
streams_application_id="streams-app-${run_id}"
streams_protocol_input_topic="streams-protocol-input-${run_id}"
streams_protocol_output_topic="streams-protocol-output-${run_id}"
streams_protocol_application_id="streams-protocol-app-${run_id}"
streams_stateful_input_topic="streams-stateful-input-${run_id}"
streams_stateful_output_topic="streams-stateful-output-${run_id}"
streams_stateful_application_id="streams-stateful-app-${run_id}"
streams_repartition_input_topic="streams-repartition-input-${run_id}"
streams_repartition_output_topic="streams-repartition-output-${run_id}"
streams_repartition_application_id="streams-repartition-app-${run_id}"
streams_standby_input_topic="streams-standby-input-${run_id}"
streams_standby_output_topic="streams-standby-output-${run_id}"
streams_standby_application_id="streams-standby-app-${run_id}"
producer_prefix="flink-producer-${run_id}-"
transfer_prefix="flink-transfer-${run_id}-"
marker="/tmp/flink-fail-once-${run_id}"
consumer_protocol_log="/tmp/rutomq-consumer-protocol-${run_id}.log"
jar="${script_dir}/target/rutomq-flink-it.jar"
legacy_admin_dir="${project_root}/tests/kafka3-admin"
legacy_admin_jar="${legacy_admin_dir}/target/rutomq-kafka3-admin-it.jar"

compose() {
  docker compose -p "${compose_project}" -f "${compose_file}" "$@"
}

cleanup() {
  rm -f "${consumer_protocol_log}"
  if [[ "${KEEP_CLUSTER:-0}" != "1" ]]; then
    compose down -v --remove-orphans
  fi
}

on_error() {
  compose logs --no-color --tail 300 rutomq jobmanager taskmanager || true
}

trap cleanup EXIT
trap on_error ERR

if [[ "${RUTOMQ_FLINK_PREBUILT:-0}" == "1" ]]; then
  test -x "${project_root}/target/release/rutomq"
  docker build \
    -f "${script_dir}/Dockerfile.prebuilt" \
    -t rutomq:flink-it \
    "${project_root}/target/release"
else
  docker build -t rutomq:flink-it "${project_root}"
fi
docker run --rm \
  -v "${script_dir}:/workspace" \
  -v rutomq-flink-m2:/root/.m2 \
  -w /workspace \
  maven:3.9.11-eclipse-temurin-21 \
  mvn -B -DskipTests \
    -Dflink.version="${flink_version}" \
    -Dflink.kafka.version="${connector_version}" \
    clean package
docker run --rm \
  -v "${legacy_admin_dir}:/workspace" \
  -v rutomq-flink-m2:/root/.m2 \
  -w /workspace \
  maven:3.9.11-eclipse-temurin-21 \
  mvn -B -DskipTests clean package

compose up -d

for _ in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:${health_port}/health/ready" >/dev/null; then
    break
  fi
  sleep 1
done
curl --fail --silent "http://127.0.0.1:${health_port}/health/ready" >/dev/null

network="${compose_project}_default"
verifier() {
  docker run --rm \
    --network "${network}" \
    -v "${jar}:/rutomq-flink-it.jar:ro" \
    eclipse-temurin:21-jdk \
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
      -cp /rutomq-flink-it.jar com.rutomq.flink.RutomqVerifier "$@"
}

verifier prepare rutomq:9092 "${input_topic}" "${output_topic}" "${partitions}"

compose exec -T jobmanager flink run /opt/flink/usrlib/rutomq-flink-it.jar \
  --mode produce \
  --bootstrap rutomq:9092 \
  --topic "${input_topic}" \
  --partitions "${partitions}" \
  --records "${records}" \
  --delay-ms 5 \
  --transactional-prefix "${producer_prefix}"

verifier verify rutomq:9092 "${input_topic}" flink- "${records}" -

compose exec -T jobmanager flink run /opt/flink/usrlib/rutomq-flink-it.jar \
  --mode transfer \
  --bootstrap rutomq:9092 \
  --input-topic "${input_topic}" \
  --output-topic "${output_topic}" \
  --group-id "${group_id}" \
  --partitions "${partitions}" \
  --delay-ms 5 \
  --transactional-prefix "${transfer_prefix}" \
  --fail-once-marker "${marker}"

verifier verify rutomq:9092 "${output_topic}" copied:flink- "${records}" "${group_id}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.ConsumerProtocolSmoke \
    rutomq:9092 "${consumer_topic}" "${consumer_group_id}" 200 \
    http://rutomq:8080/metrics \
  2>&1 | tee "${consumer_protocol_log}"
if grep -E 'invalid (full|incremental) fetch response|extraIds=' \
  "${consumer_protocol_log}" >/dev/null; then
  echo "Kafka client rejected a Fetch session response" >&2
  exit 1
fi

curl --fail --silent "http://127.0.0.1:${health_port}/metrics" \
  | grep -F 'rutomq_kafka_requests_total{api="OffsetCommit",version="10"} ' >/dev/null
curl --fail --silent "http://127.0.0.1:${health_port}/metrics" \
  | grep -F 'rutomq_kafka_requests_total{api="OffsetFetch",version="10"} ' >/dev/null

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.DeleteGroupsDuplicateSmoke \
    rutomq:9092 "${delete_groups_duplicate_topic}" "${delete_groups_duplicate_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.GroupDescribeDuplicateSmoke \
    rutomq:9092 "${group_describe_duplicate_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.ClassicStaticMembershipSmoke \
    rutomq:9092 "${static_topic}" "${static_group_id}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.ClassicJoinGroupBarrierSmoke \
    rutomq:9092 "${classic_join_barrier_group_id}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.TopicRecordAdmissionSmoke \
    rutomq:9092 "${record_admission_topic}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.ProduceMissingTopicSmoke \
    rutomq:9092 "${produce_missing_topic_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.TopicCompressionSmoke \
    rutomq:9092 "${compression_topic_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.ShareConsumerSmoke \
    rutomq:9092 "${share_topic}" "${share_group_id}" 200

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.ShareProtocolV2Smoke \
    rutomq:9092 "${share_topic}" "${share_group_id}-v2" 200

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.ShareDurationResetSmoke \
    rutomq:9092 "${share_duration_topic}" "${share_duration_group_id}"

curl --fail --silent "http://127.0.0.1:${health_port}/metrics" \
  | grep -F 'rutomq_kafka_requests_total{api="ShareFetch",version="2"} ' >/dev/null
curl --fail --silent "http://127.0.0.1:${health_port}/metrics" \
  | grep -F 'rutomq_kafka_requests_total{api="ShareAcknowledge",version="2"} ' >/dev/null
curl --fail --silent "http://127.0.0.1:${health_port}/metrics" \
  | grep -F 'rutomq_kafka_requests_total{api="DescribeShareGroupOffsets",version="1"} ' >/dev/null

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.ClientQuotaSmoke \
    rutomq:9092 "${quota_topic}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.CompactionSmoke \
    rutomq:9092 "${compaction_topic}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.MinInSyncReplicasSmoke \
    rutomq:9092 "${min_isr_topic}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.FileDeleteDelaySmoke \
    rutomq:9092 "${file_delete_delay_topic}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.TopicFlushPolicySmoke \
    rutomq:9092 "${flush_policy_topic}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.CreateTopicsAssignmentSmoke \
    rutomq:9092 "${create_topics_assignment_topic_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.CreateTopicsIdentitySmoke \
    rutomq:9092 "${create_topics_identity_topic_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.DeleteTopicsIdentitySmoke \
    rutomq:9092 "${delete_topics_identity_topic_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.DeleteTopicsAuthorizationPrivacySmoke \
    rutomq:9092 "${delete_topics_privacy_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.CreateAclsRequestValidationSmoke \
    rutomq:9092 "${create_acls_validation_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.MetadataSemanticsSmoke \
    rutomq:9092 "${metadata_semantics_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.GroupHeartbeatRecoverySmoke \
    rutomq:9092 "${group_heartbeat_recovery_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.StreamsTopologyValidationSmoke \
    rutomq:9092 "${streams_topology_validation_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.CreatePartitionsDuplicateSmoke \
    rutomq:9092 "${create_partitions_duplicate_topic_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.DescribeTopicPartitionsSmoke \
    rutomq:9092 "${describe_topic_partitions_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.BrokerConfigsSmoke \
    rutomq:9092

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.UnsupportedTopicConfigSmoke \
    rutomq:9092 "${unsupported_topic_config_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.ListOffsetsSmoke \
    rutomq:9092 "${list_offsets_topic}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.InitProducerIdTwoPhaseAclSmoke \
    rutomq:9092 "${two_phase_acl_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.InitProducerIdV6Smoke \
    rutomq:9092 "${kip939_topic}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.TransactionLegacyFencingSmoke \
    rutomq:9092 "${transaction_legacy_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.FindCoordinatorSemanticsSmoke \
    rutomq:9092

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.TransactionQuerySemanticsSmoke \
    rutomq:9092 "${transaction_query_prefix}"

curl --fail --silent "http://127.0.0.1:${health_port}/metrics" \
  | grep -F 'rutomq_kafka_requests_total{api="InitProducerId",version="6"} ' >/dev/null
curl --fail --silent "http://127.0.0.1:${health_port}/metrics" \
  | grep -F 'rutomq_kafka_requests_total{api="EndTxn",version="4"} ' >/dev/null

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.KafkaStreamsSmoke \
    rutomq:9092 "${streams_input_topic}" "${streams_output_topic}" \
    "${streams_application_id}" 200 classic

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.KafkaStreamsSmoke \
    rutomq:9092 "${streams_protocol_input_topic}" \
    "${streams_protocol_output_topic}" \
    "${streams_protocol_application_id}" 200 streams

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.KafkaStreamsStatefulSmoke \
    rutomq:9092 "${streams_stateful_input_topic}" \
    "${streams_stateful_output_topic}" \
    "${streams_stateful_application_id}" 80

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.KafkaStreamsStatefulSmoke \
    rutomq:9092 "${streams_repartition_input_topic}" \
    "${streams_repartition_output_topic}" \
    "${streams_repartition_application_id}" 80 repartition

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.KafkaStreamsStandbySmoke \
    rutomq:9092 "${streams_standby_input_topic}" \
    "${streams_standby_output_topic}" \
    "${streams_standby_application_id}" 80

curl --fail --silent "http://127.0.0.1:${health_port}/metrics" \
  | grep -F 'rutomq_kafka_requests_total{api="StreamsGroupHeartbeat",version="0"} ' >/dev/null
curl --fail --silent "http://127.0.0.1:${health_port}/metrics" \
  | grep -F 'rutomq_kafka_requests_total{api="StreamsGroupDescribe",version="0"} ' >/dev/null

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.FeatureSmoke \
    rutomq:9092

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.TelemetrySmoke \
    rutomq:9092 http://rutomq:8080/metrics

docker run --rm \
  --network "${network}" \
  -v "${jar}:/rutomq-flink-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp /rutomq-flink-it.jar com.rutomq.flink.LegacyNonTopicAlterConfigsSmoke \
    rutomq:9092 "${legacy_config_prefix}"

docker run --rm \
  --network "${network}" \
  -v "${legacy_admin_jar}:/rutomq-kafka3-admin-it.jar:ro" \
  eclipse-temurin:21-jdk \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -jar /rutomq-kafka3-admin-it.jar \
    rutomq:9092 "${legacy_config_prefix}-kafka3"

compose logs --no-color taskmanager \
  | grep -F "Injected Flink recovery failure" >/dev/null
echo "Flink ${flink_version} + Kafka connector ${connector_version}, Kafka Streams 4.2 classic/streams exactly-once, state restore/repartition, GROUP config, and standby failover, classic JoinGroup generation barrier, Produce missing-topic boundaries, KIP-1211 shared Admin/Metadata topic-creation defaults, CreateTopics virtual assignments/identity validation, DeleteTopics v6 identities and authorization privacy, CreateAcls request validation, Metadata v0/v10-v13, FindCoordinator v4-v6, and transaction query semantics, KIP-1251 consumer assignment-epoch OffsetCommit fencing, consumer/share/streams heartbeat response-loss recovery and static temporary leave, CreatePartitions duplicate validation, virtual broker/logger configs, legacy GROUP/CLIENT_METRICS replacement with Kafka 4.2 generated wire and Kafka 3.9 AdminClient, unsupported physical topic-config rejection, topic compression levels/admission, flush policies, file deletion delay, min ISR acknowledgements, KIP-939 recovery, legacy transaction fencing, and TWO_PHASE_COMMIT ACL enforcement, Fetch sessions, consumer/share v2 with duration reset, Offset v10, Admin lag, quota, compaction, ListOffsets v11, feature, and telemetry acceptance passed"
