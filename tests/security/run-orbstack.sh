#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_DIR="$ROOT/tests/security"
COMPOSE_FILE="$TEST_DIR/docker-compose.yml"
PROJECT="${RUTOMQ_SECURITY_PROJECT:-rutomq-security}"
M2_CACHE="${RUTOMQ_M2_CACHE:-rutomq-flink-m2}"
KAFKA_VERSION="${KAFKA_VERSION:-4.3.1}"

cleanup() {
  local status=$?
  if (( status != 0 )); then
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" logs --no-color --tail=300 rutomq || true
  fi
  if [[ "${KEEP_SECURITY_CLUSTER:-0}" != "1" ]]; then
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  return "$status"
}
trap cleanup EXIT

mkdir -p "$TEST_DIR/runtime"
docker run --rm \
  -v "$TEST_DIR/runtime:/certs" \
  rust:1.88-bookworm \
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
    -subj /CN=rutomq \
    -addext subjectAltName=DNS:rutomq,IP:127.0.0.1 \
    -keyout /certs/server.key \
    -out /certs/server.crt >/dev/null 2>&1

docker run --rm \
  -v "$TEST_DIR:/workspace" \
  -v "$M2_CACHE:/root/.m2" \
  -w /workspace \
  maven:3.9.11-eclipse-temurin-21 \
  mvn -q -DskipTests -Dkafka.version="$KAFKA_VERSION" package

docker build -t rutomq:security-it "$ROOT"
docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d

for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:18083/health/ready >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS http://127.0.0.1:18083/health/ready >/dev/null

run_client() {
  local mechanism="$1"
  local username="$2"
  local password="$3"
  local topic="$4"
  local action="${5:-smoke}"
  docker run --rm \
    --network "${PROJECT}_default" \
    -v "$TEST_DIR/target/rutomq-security-it.jar:/app.jar:ro" \
    -v "$TEST_DIR/runtime/server.crt:/server.crt:ro" \
    eclipse-temurin:21-jdk \
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
      -jar /app.jar rutomq:9092 /server.crt \
      "$mechanism" "$username" "$password" "$topic" "$action"
}

run_legacy_v0() {
  local mechanism="$1"
  local username="$2"
  local password="$3"
  docker run --rm \
    --network "${PROJECT}_default" \
    -v "$TEST_DIR/target/rutomq-security-it.jar:/app.jar:ro" \
    -v "$TEST_DIR/runtime/server.crt:/server.crt:ro" \
    eclipse-temurin:21-jdk \
    java -cp /app.jar com.rutomq.security.LegacySaslHandshakeV0Smoke \
      rutomq 9092 /server.crt "$mechanism" "$username" "$password"
}

run_legacy_v0 SCRAM-SHA-256 admin admin-secret
run_legacy_v0 SCRAM-SHA-512 admin admin-secret
run_client SCRAM-SHA-256 admin admin-secret security-sha256 provision-acls
run_client SCRAM-SHA-256 admin admin-secret security-sha256 metadata-version-43
run_client SCRAM-SHA-256 admin admin-secret security-sha256 describe-log-dirs-v5
run_client SCRAM-SHA-256 admin admin-secret security-sha256 kip-1240-group-configs
run_client SCRAM-SHA-256 admin admin-secret security-sha256 kip-1263-assignment-intervals
run_client SCRAM-SHA-256 admin admin-secret security-sha256 kip-1196-buffer-controls
run_client SCRAM-SHA-256 admin admin-secret security-visibility authorization-filtering
run_client SCRAM-SHA-256 alice correct-horse security-sha256
run_client SCRAM-SHA-256 alice correct-horse security-sha256 delegation-token-requester
run_client SCRAM-SHA-256 admin admin-secret security-sha256 delegation-token
run_client SCRAM-SHA-256 dynamic dynamic-secret security-sha256
run_client SCRAM-SHA-512 admin admin-secret security-sha512 provision-acls
run_client SCRAM-SHA-512 alice correct-horse security-sha512
run_client SCRAM-SHA-512 admin admin-secret security-sha512 delegation-token
run_client SCRAM-SHA-512 dynamic dynamic-secret security-sha512
if run_client SCRAM-SHA-256 bob bob-secret security-sha256 \
  >"$TEST_DIR/runtime/authorization-rejected.log" 2>&1; then
  echo "unauthorized Kafka principal was accepted" >&2
  exit 1
fi
grep -Eq 'TopicAuthorizationException|TOPIC_AUTHORIZATION_FAILED' \
  "$TEST_DIR/runtime/authorization-rejected.log"
if run_client SCRAM-SHA-256 alice wrong-password security-rejected \
  >"$TEST_DIR/runtime/rejected.log" 2>&1; then
  echo "wrong SCRAM password was accepted" >&2
  exit 1
fi
grep -q 'SaslAuthenticationException' "$TEST_DIR/runtime/rejected.log"

metrics="$(curl -fsS http://127.0.0.1:18083/metrics)"
successes="$(awk '$1 == "rutomq_sasl_authentications_total" { print int($2) }' <<<"$metrics")"
failures="$(awk '$1 == "rutomq_sasl_authentication_failures_total" { print int($2) }' <<<"$metrics")"
(( successes >= 10 ))
(( failures >= 1 ))
echo "Kafka ${KAFKA_VERSION} metadata.version 30, DescribeLogDirs v5, KIP-1240 share group configs, KIP-1263 assignment batching configs, KIP-1196 coordinator buffer controls, TLS, SaslHandshake v0/v1 SCRAM-SHA-256/512, dynamic credentials, delegation token owner defaulting, owner/requester/User ACL, renewer, and generated-wire error semantics, TWO_PHASE_COMMIT ACL management, least-privilege Fetch/list filtering, and authorization smoke passed"
