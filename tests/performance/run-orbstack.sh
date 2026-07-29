#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${script_dir}/../.." && pwd)"
compose_file="${script_dir}/docker-compose.yml"
compose_project="${RUTOMQ_PERF_PROJECT:-rutomq-performance}"
run_id="${RUTOMQ_PERF_RUN_ID:-$(date +%Y%m%dT%H%M%S)}"
records="${RUTOMQ_PERF_RECORDS:-10000}"
payload_bytes="${RUTOMQ_PERF_PAYLOAD_BYTES:-1024}"
producers="${RUTOMQ_PERF_PRODUCERS:-8}"
partitions="${RUTOMQ_PERF_PARTITIONS:-8}"
agent_counts="${RUTOMQ_PERF_AGENT_COUNTS:-1 3}"
batch_sizes="${RUTOMQ_PERF_BATCH_SIZES:-1048576 8388608}"
flush_windows="${RUTOMQ_PERF_FLUSH_WINDOWS_MS:-25 250}"
admin1_port="${RUTOMQ_PERF_ADMIN1_PORT:-38091}"
admin2_port="${RUTOMQ_PERF_ADMIN2_PORT:-38092}"
admin3_port="${RUTOMQ_PERF_ADMIN3_PORT:-38093}"
output="${RUTOMQ_PERF_OUTPUT:-${script_dir}/results/baseline-${run_id}.jsonl}"
jar="${project_root}/tests/flink/target/rutomq-flink-it.jar"
metrics_dir="$(mktemp -d -t rutomq-performance-metrics.XXXXXX)"
active_case=""

compose() {
  docker compose -p "${compose_project}" -f "${compose_file}" "$@"
}

cleanup() {
  rm -rf "${metrics_dir}"
  if [[ "${KEEP_CLUSTER:-0}" != "1" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}

on_error() {
  echo "performance case failed: ${active_case}" >&2
  compose logs --no-color --tail 250 gateway agent1 agent2 agent3 postgres minio || true
}

trap cleanup EXIT
trap on_error ERR

if [[ "${RUTOMQ_PERF_PREBUILT:-0}" == "1" ]]; then
  test -x "${project_root}/target/release/rutomq"
  docker build \
    -f "${project_root}/tests/flink/Dockerfile.prebuilt" \
    -t rutomq:performance-it \
    "${project_root}/target/release"
else
  docker build -t rutomq:performance-it "${project_root}"
fi

docker run --rm \
  -v "${project_root}/tests/flink:/workspace" \
  -v rutomq-flink-m2:/root/.m2 \
  -w /workspace \
  maven:3.9.11-eclipse-temurin-21 \
  mvn -B -DskipTests clean package

mkdir -p "$(dirname "${output}")"
: >"${output}"

metric_sum() {
  local metric="$1"
  shift
  awk -v metric="${metric}" '$1 == metric { sum += $2 } END { printf "%.9f", sum + 0 }' "$@"
}

capture_metrics() {
  local count="$1"
  local case_id="$2"
  local ports=("${admin1_port}" "${admin2_port}" "${admin3_port}")
  local files=()
  for ((index = 0; index < count; index++)); do
    local file="${metrics_dir}/${case_id}-agent-$((index + 1)).prom"
    curl --fail --silent "http://127.0.0.1:${ports[index]}/metrics" >"${file}"
    files+=("${file}")
  done
  printf '%s\n' "${files[@]}"
}

run_case() {
  local count="$1"
  local batch_size="$2"
  local flush_ms="$3"
  local case_id="a${count}-b${batch_size}-f${flush_ms}"
  local topic="performance-${run_id}-${case_id}"
  active_case="${case_id}"
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  export RUTOMQ_PERF_MAX_BATCH_BYTES="${batch_size}"
  export RUTOMQ_PERF_FLUSH_INTERVAL_MS="${flush_ms}"

  local services=(postgres minio minio-init agent1 gateway)
  if [[ "${count}" = "3" ]]; then
    services+=(agent2 agent3)
  fi
  compose up -d "${services[@]}"

  local ports=("${admin1_port}" "${admin2_port}" "${admin3_port}")
  for ((index = 0; index < count; index++)); do
    local ready=0
    for _ in $(seq 1 90); do
      if curl --fail --silent \
        "http://127.0.0.1:${ports[index]}/health/ready" >/dev/null; then
        ready=1
        break
      fi
      sleep 1
    done
    test "${ready}" = "1"
  done

  local network="${compose_project}_default"
  local java_output
  java_output="$(docker run --rm \
    --network "${network}" \
    -v "${jar}:/rutomq-flink-it.jar:ro" \
    eclipse-temurin:21-jdk \
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
      -cp /rutomq-flink-it.jar com.rutomq.flink.PerformanceSmoke \
      gateway:9092 "${topic}" "${records}" "${payload_bytes}" \
      "${producers}" "${partitions}")"
  printf '%s\n' "${java_output}"
  local benchmark
  benchmark="$(printf '%s\n' "${java_output}" | grep '^{' | tail -1)"
  test -n "${benchmark}"

  local metric_files=()
  while IFS= read -r metric_file; do
    metric_files+=("${metric_file}")
  done < <(capture_metrics "${count}" "${case_id}")
  local put_requests get_requests put_bytes get_bytes
  local cache_hits cache_misses cache_evictions
  local commit_sum commit_count put_sum put_count get_sum get_count
  put_requests="$(metric_sum \
    'rutomq_object_store_requests_total{operation="put"}' "${metric_files[@]}")"
  get_requests="$(metric_sum \
    'rutomq_object_store_requests_total{operation="get"}' "${metric_files[@]}")"
  put_bytes="$(metric_sum \
    'rutomq_object_store_bytes_total{operation="put"}' "${metric_files[@]}")"
  get_bytes="$(metric_sum \
    'rutomq_object_store_bytes_total{operation="get"}' "${metric_files[@]}")"
  cache_hits="$(metric_sum 'rutomq_fetch_cache_hits_total' "${metric_files[@]}")"
  cache_misses="$(metric_sum 'rutomq_fetch_cache_misses_total' "${metric_files[@]}")"
  cache_evictions="$(metric_sum 'rutomq_fetch_cache_evictions_total' "${metric_files[@]}")"
  commit_sum="$(metric_sum \
    'rutomq_produce_metadata_commit_duration_seconds_sum' "${metric_files[@]}")"
  commit_count="$(metric_sum \
    'rutomq_produce_metadata_commit_duration_seconds_count' "${metric_files[@]}")"
  put_sum="$(metric_sum \
    'rutomq_object_store_duration_seconds_sum{operation="put"}' "${metric_files[@]}")"
  put_count="$(metric_sum \
    'rutomq_object_store_duration_seconds_count{operation="put"}' "${metric_files[@]}")"
  get_sum="$(metric_sum \
    'rutomq_object_store_duration_seconds_sum{operation="get"}' "${metric_files[@]}")"
  get_count="$(metric_sum \
    'rutomq_object_store_duration_seconds_count{operation="get"}' "${metric_files[@]}")"

  jq -cn \
    --arg runId "${run_id}" \
    --arg caseId "${case_id}" \
    --argjson benchmark "${benchmark}" \
    --argjson agentCount "${count}" \
    --argjson batchBytes "${batch_size}" \
    --argjson flushMs "${flush_ms}" \
    --arg agentCpus "${RUTOMQ_PERF_AGENT_CPUS:-1.0}" \
    --arg agentMemory "${RUTOMQ_PERF_AGENT_MEMORY:-512m}" \
    --argjson putRequests "${put_requests}" \
    --argjson getRequests "${get_requests}" \
    --argjson putBytes "${put_bytes}" \
    --argjson getBytes "${get_bytes}" \
    --argjson cacheHits "${cache_hits}" \
    --argjson cacheMisses "${cache_misses}" \
    --argjson cacheEvictions "${cache_evictions}" \
    --argjson commitSum "${commit_sum}" \
    --argjson commitCount "${commit_count}" \
    --argjson putSum "${put_sum}" \
    --argjson putCount "${put_count}" \
    --argjson getSum "${get_sum}" \
    --argjson getCount "${get_count}" \
    '$benchmark + {
      runId: $runId,
      caseId: $caseId,
      agentCount: $agentCount,
      agentCpus: $agentCpus,
      agentMemory: $agentMemory,
      flushIntervalMs: $flushMs,
      maxBatchBytes: $batchBytes,
      metrics: {
        objectPutRequests: $putRequests,
        objectGetRequests: $getRequests,
        objectPutBytes: $putBytes,
        objectGetBytes: $getBytes,
        fetchCacheHits: $cacheHits,
        fetchCacheMisses: $cacheMisses,
        fetchCacheEvictions: $cacheEvictions,
        metadataCommitMeanMs:
          (if $commitCount > 0 then $commitSum / $commitCount * 1000 else 0 end),
        objectPutMeanMs:
          (if $putCount > 0 then $putSum / $putCount * 1000 else 0 end),
        objectGetMeanMs:
          (if $getCount > 0 then $getSum / $getCount * 1000 else 0 end)
      }
    }' | tee -a "${output}"
}

for agent_count in ${agent_counts}; do
  if [[ "${agent_count}" != "1" && "${agent_count}" != "3" ]]; then
    echo "RUTOMQ_PERF_AGENT_COUNTS supports only 1 and 3" >&2
    exit 1
  fi
  for batch_size in ${batch_sizes}; do
    for flush_ms in ${flush_windows}; do
      run_case "${agent_count}" "${batch_size}" "${flush_ms}"
    done
  done
done

echo "Performance baseline written to ${output}"
