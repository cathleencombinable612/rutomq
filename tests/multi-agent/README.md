# Multi-Agent durability acceptance

This isolated OrbStack gate runs three diskless rutomq Agents behind one TCP
service endpoint. All Agents share PostgreSQL and MinIO; none mounts a volume.

It first arms a client-ID-scoped, one-shot test failpoint on Agent 1. The
initial idempotent Produce commits its object and PostgreSQL span, then receives
EOF before a success response. The gate removes Agent 1 and resends the same
producer ID, epoch, and sequence directly through Agent 2. The retry must
return the original offset, preserve one visible span, and converge to one
physical object after orphan GC.

The remaining scenarios verify concurrent single-partition ordering,
idempotent retry after SIGKILL, restart after every Agent is removed, S3
failure before upload, PostgreSQL failure after object upload, and eventual
orphan cleanup through PostgreSQL upload intents. The failpoint is inactive
unless `RUTOMQ_TEST_PRODUCE_DISCONNECT_AFTER_COMMIT_CLIENT_ID` is explicitly
set on an Agent.

Run:

```bash
tests/multi-agent/run-orbstack.sh
```

Set `KEEP_CLUSTER=1` to retain the compose project after a run.
