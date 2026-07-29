# Retention object deletion recovery

`run-orbstack.sh` builds an isolated PostgreSQL, MinIO, and two-Agent
environment. A generated Kafka 4.2 client request writes two topics into one
immutable object. The gate verifies:

- expiry of one topic advances its log start without deleting the shared
  object;
- the other topic remains fetchable from that object;
- a MinIO delete outage leaves the committed PostgreSQL deletion obligation
  intact;
- after Agent replacement and MinIO recovery, the replacement Agent removes
  both the PostgreSQL object row and physical object;
- neither Agent mounts a volume or local WAL.

Run with OrbStack:

```bash
tests/retention/run-orbstack.sh
```

Set `RUTOMQ_RETENTION_PREBUILT=1` to package an already-built
`target/release/rutomq` binary instead of rebuilding Rust in Docker.
