# Repeatable performance baseline

`run-orbstack.sh` records a fixed-resource Kafka 4.2 baseline for one and
three stateless Agents. Each case starts fresh PostgreSQL and MinIO instances,
sets the Agent batching window and byte bound, runs concurrent idempotent
producers, verifies every record twice, and captures:

- records/s and MiB/s;
- Produce acknowledgement p50/p95/p99;
- OpenDAL PUT/GET request and byte counts;
- Fetch cache hit/miss/eviction counts;
- mean PostgreSQL Produce metadata-commit latency;
- mean object PUT/GET latency.

The default matrix is `1,3 Agents x 1,8 MiB x 25,250 ms`, with 10,000 records
of 1 KiB. Every Agent is limited to one CPU and 512 MiB. Change inputs through
the `RUTOMQ_PERF_*` environment variables; the JSONL output always records the
effective dimensions.

```bash
cargo build --release -p rutomq
RUTOMQ_PERF_PREBUILT=1 tests/performance/run-orbstack.sh
```

This is a reproducible baseline, not a throughput promise. Compare only runs
made on equivalent hardware and dependency versions.
