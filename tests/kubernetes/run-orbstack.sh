#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${script_dir}/../.." && pwd)"
chart="${project_root}/deploy/helm/rutomq"
dependencies="${script_dir}/dependencies.yaml"
run_id="${RUTOMQ_K8S_RUN_ID:-$(date +%s)}"
namespace="${RUTOMQ_K8S_NAMESPACE:-rutomq-k8s-${run_id}}"
release="${RUTOMQ_K8S_RELEASE:-rutomq}"
resource="${release}-rutomq"
bootstrap="${resource}:9092"
topic="kubernetes-${run_id}"
prefix="record"
agent_image="rutomq"
image_a="k8s-it-a"
image_b="k8s-it-b"
client_image="rutomq-kafka-client:k8s-it"
jar="${project_root}/tests/flink/target/rutomq-flink-it.jar"

cleanup() {
  if [[ "${KEEP_CLUSTER:-0}" != "1" ]]; then
    helm uninstall "${release}" -n "${namespace}" --wait >/dev/null 2>&1 || true
    kubectl delete namespace "${namespace}" --wait=false >/dev/null 2>&1 || true
  fi
}

on_error() {
  kubectl get all -n "${namespace}" -o wide || true
  kubectl get events -n "${namespace}" --sort-by=.lastTimestamp | tail -80 || true
  kubectl logs -n "${namespace}" -l app.kubernetes.io/name=rutomq \
    --all-containers --prefix --tail=200 || true
}

trap cleanup EXIT
trap on_error ERR

build_agent_images() {
  if [[ "${RUTOMQ_K8S_PREBUILT:-0}" == "1" ]]; then
    test -x "${project_root}/target/release/rutomq"
    docker build \
      -f "${project_root}/tests/flink/Dockerfile.prebuilt" \
      -t "${agent_image}:${image_a}" \
      "${project_root}/target/release"
  else
    docker build -t "${agent_image}:${image_a}" "${project_root}"
  fi
  docker tag "${agent_image}:${image_a}" "${agent_image}:${image_b}"
}

build_client_image() {
  docker run --rm \
    -v "${project_root}/tests/flink:/workspace" \
    -v rutomq-flink-m2:/root/.m2 \
    -w /workspace \
    maven:3.9.11-eclipse-temurin-21 \
    mvn -B -DskipTests clean package
  test -f "${jar}"
  docker build \
    -f "${script_dir}/Dockerfile.client" \
    -t "${client_image}" \
    "${project_root}/tests/flink"
}

create_client_job() {
  local name="$1"
  shift
  kubectl create job "${name}" -n "${namespace}" \
    --image="${client_image}" \
    -- java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
      -cp /opt/rutomq/rutomq-kafka-client.jar \
      com.rutomq.flink.MultiAgentDurabilitySmoke "$@"
  kubectl patch job "${name}" -n "${namespace}" --type=merge \
    -p '{"spec":{"backoffLimit":0}}' >/dev/null
}

wait_client_job() {
  local name="$1"
  if ! kubectl wait -n "${namespace}" \
    --for=condition=complete --timeout=240s "job/${name}"; then
    kubectl logs -n "${namespace}" "job/${name}" --all-containers || true
    return 1
  fi
  kubectl logs -n "${namespace}" "job/${name}" --all-containers
}

run_client_job() {
  local name="$1"
  shift
  create_client_job "${name}" "$@"
  wait_client_job "${name}"
  kubectl delete job "${name}" -n "${namespace}" --wait=true >/dev/null
}

helm_values=(
  --set "image.repository=${agent_image}"
  --set "image.tag=${image_a}"
  --set "image.pullPolicy=IfNotPresent"
  --set "replicaCount=3"
  --set "objectStore.endpoint=http://minio:9000"
  --set "agent.flushIntervalMs=100"
  --set "agent.maxBatchBytes=1048576"
  --set "agent.shutdownGraceMs=25000"
  --set "agent.orphanGcIntervalMs=1000"
  --set "agent.orphanGcGraceMs=5000"
  --set "pod.terminationGracePeriodSeconds=35"
)

build_agent_images
build_client_image
helm lint "${chart}"
rendered_chart="$(helm template "${release}" "${chart}" "${helm_values[@]}")"
grep -q 'RUTOMQ_MAX_BATCH_BYTES: "1048576"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_TRANSACTIONAL_ID_EXPIRATION_MS: "604800000"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS: "3600000"' \
  <<<"${rendered_chart}"
grep -q 'RUTOMQ_GROUP_MAX_SIZE: "2147483647"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_GROUP_CONSUMER_MAX_SIZE: "2147483647"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_GROUP_CONSUMER_ASSIGNORS: "uniform,range"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_GROUP_CONSUMER_REGEX_REFRESH_INTERVAL_MS: "600000"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_GROUP_SHARE_ASSIGNORS: "simple"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_STREAMS_GROUP_HEARTBEAT_INTERVAL_MS: "5000"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS: "5000"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL_MS: "15000"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_STREAMS_GROUP_SESSION_TIMEOUT_MS: "45000"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS: "45000"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS: "60000"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_GROUP_STREAMS_MAX_SIZE: "2147483647"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS: "3000"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_STREAMS_GROUP_NUM_STANDBY_REPLICAS: "0"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_STREAMS_GROUP_MAX_STANDBY_REPLICAS: "2"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_SHARE_GROUP_MAX_SIZE: "200"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_SHARE_RECORD_LOCK_DURATION_MS: "30000"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_SHARE_MIN_RECORD_LOCK_DURATION_MS: "15000"' <<<"${rendered_chart}"
grep -q 'RUTOMQ_SHARE_MAX_RECORD_LOCK_DURATION_MS: "60000"' <<<"${rendered_chart}"

kubectl create namespace "${namespace}"
kubectl apply -n "${namespace}" -f "${dependencies}"
kubectl rollout status -n "${namespace}" deployment/postgres --timeout=120s
kubectl rollout status -n "${namespace}" deployment/minio --timeout=120s
kubectl wait -n "${namespace}" \
  --for=condition=complete --timeout=120s job/minio-init

helm upgrade --install "${release}" "${chart}" -n "${namespace}" \
  --wait --timeout=5m "${helm_values[@]}"
kubectl rollout status -n "${namespace}" "deployment/${resource}" --timeout=180s

test "$(kubectl get deployment "${resource}" -n "${namespace}" \
  -o jsonpath='{.spec.template.spec.automountServiceAccountToken}')" = "false"
test -z "$(kubectl get deployment "${resource}" -n "${namespace}" \
  -o jsonpath='{.spec.template.spec.containers[0].volumeMounts}')"
test -z "$(kubectl get pvc -n "${namespace}" \
  -l app.kubernetes.io/name=rutomq -o name)"
test "$(kubectl get pdb "${resource}" -n "${namespace}" \
  -o jsonpath='{.spec.minAvailable}')" = "1"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.KAFKA_ADVERTISE_HOST}')" = "${resource}"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_OBSERVABILITY_INTERVAL_MS}')" = "15000"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_OBSERVABILITY_MAX_GROUPS}')" = "1000"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_CONSUMER_LAG_MAX_SERIES}')" = "10000"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_PARTITION_RETENTION_MAX_SERIES}')" = "10000"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_PRODUCER_ID_EXPIRATION_MS}')" = "86400000"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS}')" = "600000"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_TRANSACTIONAL_ID_EXPIRATION_MS}')" = "604800000"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS}')" = "3600000"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS}')" = "10000"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_TRANSACTION_PARTITION_VERIFICATION_ENABLE}')" = "true"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_TRANSACTION_MAX_TIMEOUT_MS}')" = "900000"
test "$(kubectl get configmap "${resource}" -n "${namespace}" \
  -o jsonpath='{.data.RUTOMQ_TRANSACTION_TWO_PHASE_COMMIT_ENABLE}')" = "false"
test "$(kubectl get deployment "${resource}" -n "${namespace}" \
  -o jsonpath='{.status.readyReplicas}')" = "3"

migration_count="$(kubectl exec -n "${namespace}" deployment/postgres -- \
  psql -U rutomq -d rutomq -Atc \
  "SELECT count(*) FROM _sqlx_migrations
   WHERE version IN (
       202607270026, 202607270027, 202607270028, 202607270029,
       202607270030, 202607270031, 202607270032, 202607270033,
       202607270034, 202607280035, 202607280036
   )")"
test "${migration_count}" = "11"

run_client_job prepare prepare "${bootstrap}" "${topic}"
run_client_job initial produce "${bootstrap}" "${topic}" "${prefix}" 0 200 4 2
run_client_job verify-initial verify "${bootstrap}" "${topic}" "${prefix}" 200

original_pods="$(kubectl get pods -n "${namespace}" \
  -l app.kubernetes.io/name=rutomq \
  -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' | sort)"
create_client_job rolling-producer produce \
  "${bootstrap}" "${topic}" "${prefix}" 200 800 8 10
sleep 2
helm upgrade "${release}" "${chart}" -n "${namespace}" \
  --reuse-values --wait --timeout=5m --set "image.tag=${image_b}"
kubectl rollout status -n "${namespace}" "deployment/${resource}" --timeout=180s
wait_client_job rolling-producer
kubectl delete job rolling-producer -n "${namespace}" --wait=true >/dev/null
run_client_job verify-rolling verify "${bootstrap}" "${topic}" "${prefix}" 1000

current_pods="$(kubectl get pods -n "${namespace}" \
  -l app.kubernetes.io/name=rutomq \
  -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' | sort)"
test -z "$(comm -12 \
  <(printf '%s\n' "${original_pods}") \
  <(printf '%s\n' "${current_pods}"))"
test "$(kubectl get pods -n "${namespace}" \
  -l app.kubernetes.io/name=rutomq \
  -o jsonpath='{range .items[*]}{.spec.containers[0].image}{"\n"}{end}' \
  | grep -c "^${agent_image}:${image_b}$")" = "3"

helm upgrade "${release}" "${chart}" -n "${namespace}" \
  --reuse-values --wait --timeout=5m --set replicaCount=5
kubectl rollout status -n "${namespace}" "deployment/${resource}" --timeout=180s
test "$(kubectl get deployment "${resource}" -n "${namespace}" \
  -o jsonpath='{.status.readyReplicas}')" = "5"
run_client_job scale-out-produce produce \
  "${bootstrap}" "${topic}" "${prefix}" 1000 300 6 0
run_client_job verify-scale-out verify \
  "${bootstrap}" "${topic}" "${prefix}" 1300

helm upgrade "${release}" "${chart}" -n "${namespace}" \
  --reuse-values --wait --timeout=5m --set replicaCount=2
kubectl rollout status -n "${namespace}" "deployment/${resource}" --timeout=180s
test "$(kubectl get deployment "${resource}" -n "${namespace}" \
  -o jsonpath='{.status.readyReplicas}')" = "2"
run_client_job scale-in-produce produce \
  "${bootstrap}" "${topic}" "${prefix}" 1300 300 6 0
run_client_job verify-final verify \
  "${bootstrap}" "${topic}" "${prefix}" 1600

pending_objects="$(kubectl exec -n "${namespace}" deployment/postgres -- \
  psql -U rutomq -d rutomq -Atc \
  "SELECT count(*) FROM objects WHERE committed = FALSE")"
test "${pending_objects}" = "0"

echo "Kubernetes rolling/scale passed records=1600 replicas=3->5->2 image=${image_b} pvc=0"
