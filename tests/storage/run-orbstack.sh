#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${script_dir}/../.." && pwd)"
compose_file="${script_dir}/docker-compose.yml"
compose_project="${RUTOMQ_STORAGE_PROJECT:-rutomq-storage-it}"

compose() {
  docker compose -p "${compose_project}" -f "${compose_file}" "$@"
}

cleanup() {
  if [[ "${KEEP_CLUSTER:-0}" != "1" ]]; then
    compose down -v --remove-orphans
  fi
}

trap cleanup EXIT
compose up -d minio
compose run --rm minio-init

docker run --rm \
  --network "${compose_project}_default" \
  -e RUTOMQ_TEST_S3_ENDPOINT=http://minio:9000 \
  -e RUTOMQ_TEST_S3_BUCKET=rutomq \
  -e RUTOMQ_TEST_S3_ACCESS_KEY_ID=minioadmin \
  -e RUTOMQ_TEST_S3_SECRET_ACCESS_KEY=minioadmin \
  -e CARGO_INCREMENTAL=0 \
  -v "${project_root}:/work:ro" \
  -v rutomq-storage-cargo-registry:/usr/local/cargo/registry \
  -v rutomq-storage-target:/work/target \
  -w /work \
  rust:1.88-bookworm \
  cargo test -p rutomq-storage --test s3_multipart -- --nocapture

docker run --rm \
  --network "${compose_project}_default" \
  -e RUTOMQ_TEST_S3_ENDPOINT=http://minio:9000 \
  -e RUTOMQ_TEST_S3_BUCKET=rutomq \
  -e RUTOMQ_TEST_S3_ACCESS_KEY_ID=minioadmin \
  -e RUTOMQ_TEST_S3_SECRET_ACCESS_KEY=minioadmin \
  -e CARGO_INCREMENTAL=0 \
  -v "${project_root}:/work:ro" \
  -v rutomq-storage-cargo-registry:/usr/local/cargo/registry \
  -v rutomq-storage-target:/work/target \
  -w /work \
  rust:1.88-bookworm \
  cargo test -p rutomq-agent large_produce_uses_multipart_and_fetches_exact_ranges -- --nocapture
