#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${script_dir}/../.." && pwd)"
compose_file="${script_dir}/docker-compose.yml"
compose_project="${RUTOMQ_CLIENTS_PROJECT:-rutomq-clients-it}"
run_id="${RUTOMQ_CLIENTS_RUN_ID:-$(date +%s)}"
health_port="${RUTOMQ_CLIENTS_HEALTH_PORT:-18084}"
export RUTOMQ_CLIENTS_HEALTH_PORT="${health_port}"

compose() {
  docker compose -p "${compose_project}" -f "${compose_file}" "$@"
}

cleanup() {
  if [[ "${KEEP_CLUSTER:-0}" != "1" ]]; then
    compose down -v --remove-orphans
  fi
}

on_error() {
  compose logs --no-color --tail 300 rutomq || true
}

trap cleanup EXIT
trap on_error ERR

docker build -t rutomq:clients-it "${project_root}"
docker build -t rutomq:clients-it-runner "${script_dir}"
compose up -d

for _ in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:${health_port}/health/ready" >/dev/null; then
    break
  fi
  sleep 1
done
curl --fail --silent "http://127.0.0.1:${health_port}/health/ready" >/dev/null

network="${compose_project}_default"
docker run --rm \
  --network "${network}" \
  rutomq:clients-it-runner \
  python /opt/rutomq-clients/librdkafka_smoke.py \
    rutomq:9092 "librdkafka-${run_id}"

docker run --rm \
  --network "${network}" \
  rutomq:clients-it-runner \
  franz-smoke rutomq:9092 "franz-${run_id}"

echo "librdkafka and franz-go client acceptance passed run=${run_id}"
