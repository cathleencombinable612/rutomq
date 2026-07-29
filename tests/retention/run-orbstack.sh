#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${script_dir}/../.." && pwd)"
compose_file="${script_dir}/docker-compose.yml"
compose_project="${RUTOMQ_RETENTION_PROJECT:-rutomq-retention-it}"
run_id="${RUTOMQ_RETENTION_RUN_ID:-$(date +%s)}"
expired_topic="retention-expired-${run_id}"
live_topic="retention-live-${run_id}"
admin1_port="${RUTOMQ_RETENTION_ADMIN1_PORT:-38281}"
admin2_port="${RUTOMQ_RETENTION_ADMIN2_PORT:-38282}"
jar="${project_root}/tests/flink/target/rutomq-flink-it.jar"

compose() {
  docker compose -p "${compose_project}" -f "${compose_file}" "$@"
}

cleanup() {
  if [[ "${KEEP_CLUSTER:-0}" != "1" ]]; then
    compose down -v --remove-orphans
  fi
}

on_error() {
  compose logs --no-color --tail 300 agent1 agent2 postgres minio || true
}

trap cleanup EXIT
trap on_error ERR

if [[ "${RUTOMQ_RETENTION_PREBUILT:-0}" == "1" ]]; then
  test -x "${project_root}/target/release/rutomq"
  docker build \
    -f "${project_root}/tests/flink/Dockerfile.prebuilt" \
    -t rutomq:retention-it \
    "${project_root}/target/release"
else
  docker build -t rutomq:retention-it "${project_root}"
fi

docker run --rm \
  -v "${project_root}/tests/flink:/workspace" \
  -v rutomq-flink-m2:/root/.m2 \
  -w /workspace \
  maven:3.9.11-eclipse-temurin-21 \
  mvn -B -DskipTests clean package

compose up -d agent1
agent1_id="$(compose ps -q agent1)"
test "$(docker inspect "${agent1_id}" --format '{{len .Mounts}}')" = "0"

for _ in $(seq 1 120); do
  if curl --fail --silent "http://127.0.0.1:${admin1_port}/health/ready" >/dev/null; then
    break
  fi
  sleep 0.25
done
curl --fail --silent "http://127.0.0.1:${admin1_port}/health/ready" >/dev/null

network="${compose_project}_default"
java_run() {
  docker run --rm \
    --network "${network}" \
    -v "${jar}:/rutomq-flink-it.jar:ro" \
    eclipse-temurin:21-jdk \
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
      -cp /rutomq-flink-it.jar \
      com.rutomq.flink.RetentionDeletionRecoverySmoke "$@"
}

psql_value() {
  compose exec -T postgres \
    psql -U rutomq -d rutomq -Atc "$1"
}

minio_count() {
  docker run --rm \
    --network "${network}" \
    --entrypoint /bin/sh \
    minio/mc:latest \
    -c "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null &&
        mc find local/rutomq" \
    | awk 'NF { count++ } END { print count + 0 }'
}

java_run prepare agent1:9092 "${expired_topic}" "${live_topic}"

shared_ready=0
for _ in $(seq 1 100); do
  expired_spans="$(psql_value \
    "SELECT count(*) FROM object_spans s
     JOIN topics t ON t.id = s.topic_id
     WHERE t.name = '${expired_topic}'")"
  live_spans="$(psql_value \
    "SELECT count(*) FROM object_spans s
     JOIN topics t ON t.id = s.topic_id
     WHERE t.name = '${live_topic}'")"
  committed_objects="$(psql_value \
    "SELECT count(*) FROM objects WHERE committed = TRUE")"
  if [[ "${expired_spans}:${live_spans}:${committed_objects}" = "0:1:1" ]]; then
    shared_ready=1
    break
  fi
  sleep 0.1
done
test "${shared_ready}" = "1"
test "$(minio_count)" = "1"
java_run verify-expired agent1:9092 "${expired_topic}"
java_run verify-live agent1:9092 "${live_topic}"

java_run expire agent1:9092 "${live_topic}"
for _ in $(seq 1 100); do
  live_spans="$(psql_value \
    "SELECT count(*) FROM object_spans s
     JOIN topics t ON t.id = s.topic_id
     WHERE t.name = '${live_topic}'")"
  if [[ "${live_spans}" = "0" ]]; then
    break
  fi
  sleep 0.05
done
test "${live_spans}" = "0"
compose stop minio

delete_failed=0
for _ in $(seq 1 80); do
  if curl --fail --silent "http://127.0.0.1:${admin1_port}/metrics" \
    | grep -E 'rutomq_retention_errors_total [1-9]' >/dev/null; then
    delete_failed=1
    break
  fi
  sleep 0.25
done
test "${delete_failed}" = "1"
pending_delete="$(psql_value \
  "SELECT count(*) FROM objects
   WHERE committed = TRUE
     AND unreferenced_at IS NOT NULL
     AND NOT EXISTS (
       SELECT 1 FROM object_spans s
       WHERE s.object_key = objects.object_key
     )")"
test "${pending_delete}" = "1"

compose stop agent1
compose rm -f agent1
compose start minio
compose run --rm minio-init >/dev/null
compose up -d agent2
agent2_id="$(compose ps -q agent2)"
test "$(docker inspect "${agent2_id}" --format '{{len .Mounts}}')" = "0"
for _ in $(seq 1 120); do
  if curl --fail --silent "http://127.0.0.1:${admin2_port}/health/ready" >/dev/null; then
    break
  fi
  sleep 0.25
done
curl --fail --silent "http://127.0.0.1:${admin2_port}/health/ready" >/dev/null

for _ in $(seq 1 120); do
  committed_objects="$(psql_value \
    "SELECT count(*) FROM objects WHERE committed = TRUE")"
  if [[ "${committed_objects}" = "0" ]]; then
    break
  fi
  sleep 0.1
done
test "${committed_objects}" = "0"
test "$(minio_count)" = "0"
java_run verify-expired agent2:9092 "${live_topic}"

echo "Retention deletion recovery passed sharedObjectSafe=true persistedRetry=true replacementAgent=true objects=0"
