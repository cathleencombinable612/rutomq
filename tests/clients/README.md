# librdkafka and franz-go acceptance

`run-orbstack.sh` starts an isolated PostgreSQL, MinIO, and diskless rutomq
Agent, then runs two non-Java Kafka client families against the same broker:

- `confluent-kafka` 2.15.0, which reports and exercises its bundled
  librdkafka runtime;
- franz-go 1.21.5.

Each client creates and describes topics, performs exact multi-partition
idempotent Produce/Fetch, commits consumer-group offsets and resumes after a
client restart, then proves committed and aborted transaction visibility under
`read_committed` and `read_uncommitted`.

Run:

```bash
tests/clients/run-orbstack.sh
```

The Compose project and all ephemeral data are removed on exit unless
`KEEP_CLUSTER=1` is set.
