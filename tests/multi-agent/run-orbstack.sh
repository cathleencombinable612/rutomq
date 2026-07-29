#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${script_dir}/../.." && pwd)"
compose_file="${script_dir}/docker-compose.yml"
compose_project="${RUTOMQ_MULTI_AGENT_PROJECT:-rutomq-multi-agent-it}"
run_id="${RUTOMQ_MULTI_AGENT_RUN_ID:-$(date +%s)}"
topic="multi-agent-${run_id}"
ambiguous_topic="${topic}-ambiguous"
prefix="record"
ambiguous_prefix="ambiguous"
initial_records="${RUTOMQ_MULTI_AGENT_INITIAL_RECORDS:-600}"
final_records="${RUTOMQ_MULTI_AGENT_FINAL_RECORDS:-300}"
expected_records=$((initial_records + final_records))
admin1_port="${RUTOMQ_MULTI_AGENT_ADMIN1_PORT:-38081}"
admin2_port="${RUTOMQ_MULTI_AGENT_ADMIN2_PORT:-38082}"
admin3_port="${RUTOMQ_MULTI_AGENT_ADMIN3_PORT:-38083}"
jar="${project_root}/tests/flink/target/rutomq-flink-it.jar"
producer_log="$(mktemp -t rutomq-multi-agent-producer.XXXXXX)"

compose() {
  docker compose -p "${compose_project}" -f "${compose_file}" "$@"
}

cleanup() {
  rm -f "${producer_log}"
  if [[ "${KEEP_CLUSTER:-0}" != "1" ]]; then
    compose down -v --remove-orphans
  fi
}

on_error() {
  compose logs --no-color --tail 300 gateway agent1 agent2 agent3 postgres minio || true
}

trap cleanup EXIT
trap on_error ERR

if [[ "${RUTOMQ_MULTI_AGENT_PREBUILT:-0}" == "1" ]]; then
  test -x "${project_root}/target/release/rutomq"
  docker build \
    -f "${project_root}/tests/flink/Dockerfile.prebuilt" \
    -t rutomq:multi-agent-it \
    "${project_root}/target/release"
else
  docker build -t rutomq:multi-agent-it "${project_root}"
fi

docker run --rm \
  -v "${project_root}/tests/flink:/workspace" \
  -v rutomq-flink-m2:/root/.m2 \
  -w /workspace \
  maven:3.9.11-eclipse-temurin-21 \
  mvn -B -DskipTests clean package

compose up -d
network="${compose_project}_default"

java_run() {
  docker run --rm \
    --network "${network}" \
    -v "${jar}:/rutomq-flink-it.jar:ro" \
    eclipse-temurin:21-jdk \
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
      -cp /rutomq-flink-it.jar com.rutomq.flink.MultiAgentDurabilitySmoke "$@"
}

ambiguous_run() {
  docker run --rm \
    --network "${network}" \
    -v "${jar}:/rutomq-flink-it.jar:ro" \
    eclipse-temurin:21-jdk \
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
      -cp /rutomq-flink-it.jar com.rutomq.flink.AmbiguousProduceAckSmoke "$@"
}

ready=0
for _ in $(seq 1 60); do
  if java_run prepare gateway:9092 "${topic}" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
test "${ready}" = "1"
java_run prepare gateway:9092 "${ambiguous_topic}"

for agent in agent1 agent2 agent3; do
  container_id="$(compose ps -q "${agent}")"
  test "$(docker inspect "${container_id}" --format '{{len .Mounts}}')" = "0"
done

for port in "${admin1_port}" "${admin2_port}" "${admin3_port}"; do
  ready=0
  for _ in $(seq 1 60); do
    if curl --fail --silent "http://127.0.0.1:${port}/health/ready" >/dev/null; then
      ready=1
      break
    fi
    sleep 1
  done
  test "${ready}" = "1"
done

ambiguous_first="$(ambiguous_run first agent1:9092 "${ambiguous_topic}" \
  "${ambiguous_prefix}-0")"
echo "${ambiguous_first}"
producer_id="$(sed -n 's/.*producer_id=\([-0-9]*\).*/\1/p' <<<"${ambiguous_first}")"
producer_epoch="$(sed -n 's/.*producer_epoch=\([-0-9]*\).*/\1/p' <<<"${ambiguous_first}")"
test -n "${producer_id}"
test -n "${producer_epoch}"

ambiguous_state="$(compose exec -T postgres psql -U rutomq -d rutomq -At -F : -c \
  "SELECT count(*), COALESCE(min(s.base_offset), -1),
          COALESCE(max(s.last_offset), -1), count(DISTINCT s.object_key)
   FROM object_spans s
   JOIN topics t ON t.id = s.topic_id
   WHERE t.name = '${ambiguous_topic}' AND s.partition_index = 0")"
test "${ambiguous_state}" = "1:0:0:1"

compose stop agent1
compose rm -f agent1
ambiguous_run retry agent2:9092 "${ambiguous_topic}" "${ambiguous_prefix}-0" \
  "${producer_id}" "${producer_epoch}" 0

ambiguous_state="$(compose exec -T postgres psql -U rutomq -d rutomq -At -F : -c \
  "SELECT count(*), COALESCE(min(s.base_offset), -1),
          COALESCE(max(s.last_offset), -1), count(DISTINCT s.object_key)
   FROM object_spans s
   JOIN topics t ON t.id = s.topic_id
   WHERE t.name = '${ambiguous_topic}' AND s.partition_index = 0")"
test "${ambiguous_state}" = "1:0:0:1"
ambiguous_next_offset="$(compose exec -T postgres psql -U rutomq -d rutomq -Atc \
  "SELECT p.next_offset
   FROM partitions p JOIN topics t ON t.id = p.topic_id
   WHERE t.name = '${ambiguous_topic}' AND p.partition_index = 0")"
test "${ambiguous_next_offset}" = "1"

pending_objects=-1
for _ in $(seq 1 100); do
  pending_objects="$(compose exec -T postgres psql -U rutomq -d rutomq -Atc \
    "SELECT count(*) FROM objects WHERE committed = FALSE")"
  if [[ "${pending_objects}" = "0" ]]; then
    break
  fi
  sleep 0.2
done
test "${pending_objects}" = "0"
ambiguous_committed_objects="$(compose exec -T postgres psql -U rutomq -d rutomq -Atc \
  "SELECT count(*) FROM objects WHERE committed = TRUE")"
test "${ambiguous_committed_objects}" = "1"
ambiguous_s3_objects="$(docker run --rm \
  --network "${network}" \
  --entrypoint /bin/sh \
  minio/mc:latest \
  -c "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null &&
      mc find local/rutomq/rutomq/data/rutomq-cluster | wc -l")"
test "${ambiguous_s3_objects}" = "${ambiguous_committed_objects}"
java_run verify gateway:9092 "${ambiguous_topic}" "${ambiguous_prefix}" 1

compose up -d agent1
for _ in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:${admin1_port}/health/ready" >/dev/null; then
    break
  fi
  sleep 1
done
curl --fail --silent "http://127.0.0.1:${admin1_port}/health/ready" >/dev/null
java_run verify gateway:9092 "${ambiguous_topic}" "${ambiguous_prefix}" 1

java_run produce gateway:9092 "${topic}" "${prefix}" \
  0 "${initial_records}" 6 5 >"${producer_log}" 2>&1 &
producer_pid=$!
sleep 1
compose kill agent1
wait "${producer_pid}"
cat "${producer_log}"
compose up -d agent1
java_run verify gateway:9092 "${topic}" "${prefix}" "${initial_records}"

curl --fail --silent "http://127.0.0.1:${admin2_port}/metrics" \
  | grep -E 'rutomq_kafka_requests_total\{api="Produce",version="[0-9]+"\} [1-9]' >/dev/null
curl --fail --silent "http://127.0.0.1:${admin3_port}/metrics" \
  | grep -E 'rutomq_kafka_requests_total\{api="Produce",version="[0-9]+"\} [1-9]' >/dev/null

compose stop agent1 agent2 agent3
compose up -d agent1 agent2 agent3
java_run verify gateway:9092 "${topic}" "${prefix}" "${initial_records}"

compose stop minio
for port in "${admin1_port}" "${admin2_port}" "${admin3_port}"; do
  curl --fail --silent "http://127.0.0.1:${port}/health/live" >/dev/null
done
java_run expect-failure gateway:9092 "${topic}" s3-unavailable
compose start minio

for _ in $(seq 1 30); do
  if curl --fail --silent "http://127.0.0.1:${admin1_port}/health/ready" >/dev/null; then
    break
  fi
  sleep 1
done
curl --fail --silent "http://127.0.0.1:${admin1_port}/health/ready" >/dev/null

compose exec -T postgres psql -v ON_ERROR_STOP=1 -U rutomq -d rutomq -c \
  "CREATE OR REPLACE FUNCTION rutomq_fail_span_insert()
   RETURNS trigger LANGUAGE plpgsql AS \$\$
   BEGIN
     RAISE EXCEPTION 'injected metadata commit failure';
   END;
   \$\$"
compose exec -T postgres psql -v ON_ERROR_STOP=1 -U rutomq -d rutomq -c \
  "CREATE TRIGGER rutomq_fail_span_insert
   BEFORE INSERT ON object_spans
   FOR EACH ROW EXECUTE FUNCTION rutomq_fail_span_insert()"
java_run expect-failure gateway:9092 "${topic}" postgres-commit-failure
compose exec -T postgres psql -v ON_ERROR_STOP=1 -U rutomq -d rutomq -c \
  "DROP TRIGGER rutomq_fail_span_insert ON object_spans"
compose exec -T postgres psql -v ON_ERROR_STOP=1 -U rutomq -d rutomq -c \
  "DROP FUNCTION rutomq_fail_span_insert()"

pending_objects=-1
for _ in $(seq 1 100); do
  pending_objects="$(compose exec -T postgres psql -U rutomq -d rutomq -Atc \
    "SELECT count(*) FROM objects WHERE committed = FALSE")"
  if [[ "${pending_objects}" = "0" ]]; then
    break
  fi
  sleep 0.2
done
test "${pending_objects}" = "0"

orphan_deleted=0
for port in "${admin1_port}" "${admin2_port}" "${admin3_port}"; do
  if curl --fail --silent "http://127.0.0.1:${port}/metrics" \
    | grep -E 'rutomq_orphan_gc_deleted_total [1-9]' >/dev/null; then
    orphan_deleted=1
  fi
done
test "${orphan_deleted}" = "1"
java_run verify gateway:9092 "${topic}" "${prefix}" "${initial_records}"

java_run produce gateway:9092 "${topic}" "${prefix}" \
  "${initial_records}" "${final_records}" 6 0
java_run verify gateway:9092 "${topic}" "${prefix}" "${expected_records}"

compose stop agent1 agent2 agent3
compose up -d agent1 agent2 agent3
java_run verify gateway:9092 "${topic}" "${prefix}" "${expected_records}"

committed_objects="$(compose exec -T postgres psql -U rutomq -d rutomq -Atc \
  "SELECT count(*) FROM objects WHERE committed = TRUE")"
invalid_integrity_spans="$(compose exec -T postgres psql -U rutomq -d rutomq -Atc \
  "SELECT count(*) FROM object_spans
   WHERE format_version <> 1 OR checksum IS NULL OR octet_length(checksum) <> 32")"
test "${invalid_integrity_spans}" = "0"
s3_objects="$(docker run --rm \
  --network "${network}" \
  --entrypoint /bin/sh \
  minio/mc:latest \
  -c "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null &&
      mc find local/rutomq/rutomq/data/rutomq-cluster | wc -l")"
test "${s3_objects}" = "${committed_objects}"

echo "Multi-Agent durability passed agents=3 records=${expected_records} committedObjects=${committed_objects} integritySpans=v1 orphanObjects=0"
