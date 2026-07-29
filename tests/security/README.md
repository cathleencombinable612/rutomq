# Kafka security compatibility

`run-orbstack.sh` builds rutomq with Rust 1.88 and starts PostgreSQL, MinIO, and
one TLS-only agent. A Kafka 4.3.1 client then verifies:

- certificate hostname validation over `SASL_SSL`;
- raw `SaslHandshake` v0 opaque-token SCRAM-SHA-256/512 followed by an
  authenticated Kafka request on the same TLS connection;
- SCRAM-SHA-256 and SCRAM-SHA-512 authentication;
- delegation-token create, renew, describe, expire, token-SCRAM authentication,
  cross-owner requester lifecycle eligibility, v3 null/empty owner-name
  defaulting, and full-principal User `CREATE_TOKENS`/`DESCRIBE_TOKENS` ACLs;
- delegation-token duplicate/empty renewer preservation and token-authenticated
  Create/Renew/Expire generated-wire error identities/timestamp sentinels;
- authenticated Admin, Produce, and Fetch requests;
- `metadata.version` support through Kafka 4.3 level 30, explicit finalization
  from 29, feature-epoch advancement, and safe/unsafe downgrade rejection;
- exact `DescribeLogDirs` v5 negotiation, including the non-cordoned virtual
  object-store directory and unknown capacity;
- Kafka 4.3 KIP-1240 GROUP config SET/Describe round trips for delivery count,
  per-partition record locks, and renew enablement, plus broker-bound rejection;
- Kafka 4.3 KIP-1263 cluster-wide BROKER assignment-default and consumer,
  share, and Streams GROUP override SET/Describe/DELETE round trips, exact
  config sources/synonyms, validate-only behavior, and bound rejection;
- Kafka `CreateAcls`, `DescribeAcls`, and `DeleteAcls` wire compatibility;
- Topic, Group, Cluster, and TransactionalId ACL matching with deny precedence;
- mixed allowed/denied Produce, Fetch, ListOffsets, OffsetForLeaderEpoch,
  DeleteRecords, OffsetDelete, and OffsetCommit requests over official Kafka
  4.3 generated wire and Admin clients, including fetched-before-denied
  response assembly and empty denied records;
- restricted all-topic OffsetFetch visibility through Kafka 4.3
  `listConsumerGroupOffsets`;
- AddPartitionsToTxn v3 complete-request authorization: a mixed denied/allowed
  request returns `TOPIC_AUTHORIZATION_FAILED` and
  `OPERATION_NOT_ATTEMPTED` without partial registration, followed by a
  successful authorized retry;
- mixed-topic TxnOffsetCommit denial, same-transactional-id producer
  reinitialization, and authorized offset commit through the official Kafka
  4.3 transactional producer;
- a Kafka 4.3 idempotent producer initialized with only Topic
  `DESCRIBE`/`WRITE`, without Cluster `IDEMPOTENT_WRITE`;
- rejection of a correctly authenticated but unauthorized principal;
- rejection of an invalid password;
- successful and failed authentication metrics.

Run:

```bash
tests/security/run-orbstack.sh
```
