# Kafka compatibility matrix

`ApiVersions` advertises the versions handled by the current agent except for
Kafka 4.3's required KAFKA-18659 Produce compatibility range: Produce 0-13 is
advertised while removed versions 0-2 remain rejected. The wire types are
generated from the official Kafka 4.3.0 schemas and vendored as
`kafka-protocol 0.17.0-kafka.4.3.0`; request and response bodies are never
decoded with a hand-maintained protocol schema.

| API | Advertised versions | Current behavior |
| --- | ---: | --- |
| ApiVersions | 0-4 | Implemented API versions plus PostgreSQL-backed Kafka 4.3 logical feature metadata: supported `metadata.version` 22-30 with level 29 finalized until an explicit rolling-upgrade completion, `transaction.version` 0-2, and `group.version`/`share.version`/`streams.version` 0-1; v3 filters zero-minimum supported ranges, v4 returns all ranges, physical KRaft/ELR features remain absent, and unsupported future request versions receive a v0 `UNSUPPORTED_VERSION` response with the complete API table so negotiation can retry on the same connection |
| Produce | 0-13 advertised; 3-13 accepted | Kafka 4.3 KAFKA-18659 librdkafka negotiation workaround without decoding removed v0-v2 schemas; batches unique Topic `WRITE` authorization before name-based metadata lookup, resolves v13 topic IDs first, returns request-wide authorizer backend errors before mutation, requires TransactionalId `WRITE` only for transactional batches, does not repeat InitProducerId's Cluster idempotence check, and closes erroneous `acks=0` requests; bounded-memory object flush with per-topic `flush.messages`/`flush.ms` triggers, no implicit topic creation, independent mixed-partition admission, Kafka-compatible `INVALID_RECORD` rejection of more than one magic-v2 batch in a partition payload, nonzero client batch base offsets, non-positive/inconsistent raw count-offset ranges, negative producer base sequences, and keyless records when cleanup policy contains `compact`, all before topic rewriting; v12+ transaction-v2 registration, active producer deduplication over Kafka's five newest exact batch sequence ranges with idle expiration, log-truncation reconciliation, and ongoing-transaction protection, virtual-ISR required-acks validation, final codec/level policy, record size/timestamp admission, LogAppendTime rewrite/CRC, and metadata commit before response |
| Fetch | 4-18 | Resolves v13+ topic IDs, batches every unique resolvable Topic `READ` authorization before metadata/object work, authorizes names before existence lookup, returns Kafka's v4-v12 partition plus top-level and v13+ top-level-only request errors on backend failure, collapses duplicate topic-partitions with last-data semantics, emits fetched partitions before accumulated authorization/metadata errors while preserving contiguous topic grouping, uses invalid error watermarks, and maps storage failures to `NOT_LEADER_OR_FOLLOWER` through v5; PostgreSQL span lookup, SHA-256 verification of format-v1 OpenDAL ranges before decode/cache, a bounded Agent-local immutable-range LRU, offset/CRC rewrite preserving the stored final codec, legacy format-v0 rolling-upgrade reads, preserved sparse offsets for compacted spans, min-byte long polling, global/partition byte limits, offset-range errors, virtual leader-epoch fencing, `read_committed` filtering, v16+ virtual node endpoints only for leader errors, explicit rejection of physical follower fetches, and bounded v7+ full/incremental sessions with forgotten, close, epoch, and topic-ID-mode semantics |
| ListOffsets | 1-11 | Batched Topic `DESCRIBE` authorization with request-wide backend errors and authorized-before-denied responses; duplicate authorized partitions collapse to one `INVALID_REQUEST`; negative selectors enforce their v7/v8/v9/v11 introduction versions with `UNSUPPORTED_VERSION`; errors retain an unknown leader epoch; successful exact record-timestamp, earliest/latest/max-timestamp, non-tiered local/remote selector, fencing, v10+ timeout, and HWM/LSO isolation semantics |
| Metadata | 0-13 | One virtual broker and stable advertised endpoint; v0 empty-list/all-topic behavior, request-wide v10/v11 topic-ID/null-name validation, v12+ topic-ID-first routing and unknown-ID privacy, Describe-before-existence/auto-create authorization, KIP-1211 virtual-controller partition defaults and the static auto-create switch, invalid-name rejection, and requested topic/cluster authorized-operation bitfields |
| DescribeCluster | 0-2 | Stable cluster ID and virtual broker/controller identity without requiring Cluster `DESCRIBE`; requested authorized operations are zero when `DESCRIBE` is denied, authorizer failures remain top-level server errors, and endpoint validation distinguishes v1+ controller mismatch from unknown endpoint types while preserving v0 compatibility |
| DescribeQuorum | 0-2 | Cluster-authorized virtual KRaft metadata quorum with controller/voter 0, stable directory ID, and advertised v2 endpoint |
| UpdateFeatures | 0-2 | Atomic Kafka 4.3 `metadata.version` 22-30, `transaction.version` 0-2, and `group.version`/`share.version`/`streams.version` 0-1 updates with validate-only, upgrade/downgrade validation, Cluster/Alter ACLs, and a monotonic feature epoch; the explicit 29-to-30 finalization is accepted while safe and unsafe downgrade across Kafka 4.3's metadata-changing level 30 boundary is rejected |
| GetTelemetrySubscriptions | 0 | Assigns client instance IDs, evaluates PostgreSQL-backed KIP-714 subscriptions against connection/client attributes, and returns deterministic subscription IDs, compression choices, intervals, limits, and metric prefixes |
| PushTelemetry | 0 | Validates subscription IDs, cadence, compression, termination, and payload limits; accepts opaque compressed OTLP payloads into broker metrics without claiming an external telemetry exporter |
| CreateTopics | 2-7 | Atomic PostgreSQL topic/config creation; request-wide 10,000-partition policy guard including configured defaults; Kafka name/reserved-name/dot-underscore collision and existing-topic validation; duplicate-request rejection; KIP-1211 `-1` virtual-controller defaults shared with Metadata auto-creation; exact partition, one-broker replication-factor, and zero-based manual `[0]` assignment validation; supported config validation and validate-only; unsupported physical-log settings return `INVALID_CONFIG` without creating that topic while unrelated topics continue; v5+ sorted effective config, source, partition, replication, and config-authorization metadata; and v7 topic IDs |
| DeleteTopics | 1-6 | Name and v6 topic-ID deletion with request-wide identity/duplicate validation, pre-existence Describe/Delete authorization, post-authorization alias checks, ID-private errors, immutable-ID targeting, shuffled mixed responses, and persisted `file.delete.delay.ms` deadlines |
| DeleteRecords | 0-2 | Authorizes every unique Topic `DELETE` resource before mutation, returns request-wide per-partition server errors without advancing any log start on authorization backend failure, preserves explicit per-topic denials, performs monotonic log-start advancement and high-watermark truncation with offset `-1`, stops before pending transaction spans, atomically rebases or removes producer retry state from retained spans, and persists per-topic delayed shared-object reclamation |
| CreatePartitions | 0-3 | Atomic partition-count increase, validate-only, virtual-broker assignment validation, and request-wide duplicate-topic rejection before authorization or mutation while unrelated unique topics continue |
| DescribeTopicPartitions | 0 | Topic metadata and authorized operations with authorization-before-existence privacy, hidden denied names in all-topic mode, zero-ID explicit authorization errors, request-wide cursor validation, and a resumable page clamped to `1..=max.request.partition.size.limit` |
| DescribeConfigs | 1-4 | Describes supported topic retention/cleanup/record-admission/object-deletion settings with PostgreSQL-persisted dynamic/default sources and Kafka-ordered broker-default synonyms, persisted CLIENT_METRICS subscriptions with source 7, Kafka 4.3 GROUP configs including bounded consumer/share/Streams heartbeat and session overrides, bounded Streams standby replicas and share lock durations, the Streams initial rebalance delay, KIP-1240 controls, and KIP-1263 assignment intervals and assignor-offload overrides with dynamic/default source 8 synonyms, cluster-wide dynamic BROKER assignment/offload defaults plus the read-only timeout/standby/lock bounds and KIP-1196 group/share cached-buffer limits, the static background-worker count, KIP-1237's deprecated read-only rebalance-protocol list, read-only transaction timeout/two-phase/transactional-ID expiration and cleanup-interval settings, KIP-1211 topic-creation defaults and auto-create switch, and truthful per-broker/BROKER_LOGGER runtime settings without physical-log claims |
| ListConfigResources | 0-1 | v0 lists persisted CLIENT_METRICS resources; v1 filters GROUP, CLIENT_METRICS, BROKER_LOGGER, BROKER, and TOPIC resources under Cluster/DescribeConfigs; GROUP contains only IDs with dynamic group configuration and excludes active/offset-only groups without config; every returned resource is accepted by DescribeConfigs |
| AlterConfigs | 0-2 | Legacy full replacement of supported TOPIC, GROUP, CLIENT_METRICS, and empty-name cluster-wide BROKER settings, including KIP-1196 cached-buffer limits, with omitted dynamic keys deleted atomically, empty Group/ClientMetrics replacements removed, Kafka-ordered duplicate/key/null/type preprocessing, complete-request authorization before mutation, request-wide valid-resource `UNKNOWN_SERVER_ERROR` on authorizer failure, per-resource ACL denial, and validate-only; unsupported physical-log settings and named physical BROKER mutation return `INVALID_CONFIG` |
| IncrementalAlterConfigs | 0-1 | SET/DELETE topic retention, record-admission, `file.delete.delay.ms`, CLIENT_METRICS, GROUP, empty-name KIP-1263 BROKER assignment defaults, and KIP-1196 cached-buffer limits plus APPEND/SUBTRACT for topic `cleanup.policy`; preprocesses duplicate/key/null/type errors, authorizes every valid resource before mutation, preserves preprocessing errors on backend failure, returns per-resource ACL denials, validates each resource atomically, and honors validate-only |
| AlterReplicaLogDirs | 1-2 | Validates replica placement against the single `/rutomq/object-store` virtual directory; matching moves are no-ops |
| DescribeLogDirs | 1-5 | Reports the virtual object-store directory, logical object-span bytes per partition, zero replica lag, unknown object-store capacity, and Kafka 4.3 v5 `is_cordoned=false` because the stateless object-store path cannot be cordoned as a local broker volume |
| ElectLeaders | 0-2 | Validates preferred/unclean election requests against the fixed virtual leader and returns `ELECTION_NOT_NEEDED` for already-led partitions |
| AlterPartitionReassignments | 0-1 | Accepts the fixed replica set `[0]` as an immediate no-op, rejects physical replica changes, preserves the v1 replication-factor-change flag, and reports absent cancellations |
| ListPartitionReassignments | 0 | Returns no active moves because virtual replica placement is immediate and immutable |
| FindCoordinator | 0-6 | Preserves exact v4+ batched key cardinality, including empty and duplicate batches; recognizes only group `0`, transaction `1`, and share `2`; rejects share lookup before v6, then requires Cluster `CLUSTER_ACTION` and a valid `groupId:base64TopicId:partition` key; applies Group/TransactionalId `DESCRIBE` ACLs and returns a non-routable no-node identity for every validation, authorization, or backend error |
| OffsetCommit | 2-10 | Batches Group/Topic authorization before mutation, validates denied/missing/invalid partitions independently, limits member/coordinator errors to valid authorized partitions, and supports PostgreSQL or memory upsert with name-based v2-v9 and topic-ID v10 routing; metadata beyond `offset.metadata.max.bytes` follows Kafka's Java `String.length()`/UTF-16-code-unit behavior and returns per-partition `OFFSET_METADATA_TOO_LARGE` without rejecting or mutating valid siblings; v2-v4 persist a non-default custom retention deadline while v5+ use broker retention semantics; v6+ persists the supplied committed leader epoch; KIP-1251 consumer members require v9+, persist an assignment epoch for assigned and pending-revocation partitions, accept a prior member epoch only for partitions whose assignment epoch permits it, and atomically publish accepted partitions while returning `STALE_MEMBER_EPOCH` only for moved partitions |
| OffsetFetch | 1-10 | Versioned request errors, batched Group/Topic authorization, authorized-before-denied explicit results, filtered all-topic privacy, name-based v1-v9 and topic-ID v10 routing, partition validation, KIP-848 member validation, and exact committed leader epoch round trips in v5+ while v1-v4 retain the schema default `-1` |
| JoinGroup | 0-9 | Enforces the broker's classic `group.min.session.timeout.ms`/`group.max.session.timeout.ms` bounds with `INVALID_SESSION_TIMEOUT` and `group.max.size` under the shared coordinator lock with `GROUP_MAX_SIZE_REACHED` only for a new member after expiry; PostgreSQL-backed v4+ pending member-ID handshake and a shared rebalance UUID/deadline/member-token barrier; responses wait for the initial gathering delay and required rejoins, any Agent can complete the generation exactly once, session expiry releases stale dynamic processes before the longer rebalance timeout, timeout retains static members, and static replacement, fencing, common-protocol voting, leader member lists, metadata refresh, and v9 assignment-skip semantics are preserved; an empty consumer-protocol group converts atomically to classic while retaining offsets and GROUP config, but active and temporarily departed static consumer members remain protected |
| SyncGroup | 0-5 | Persists leader-provided opaque assignments, waits follower requests until their assignment is available, returns `REBALANCE_IN_PROGRESS` during JoinGroup assembly, and fences stale static identities |
| Heartbeat | 0-4 | Atomically expires dead members, advances stable-group generations, returns `REBALANCE_IN_PROGRESS` during JoinGroup assembly, validates/refreshes active members, and fences replaced static identities |
| LeaveGroup | 0-5 | Removes members, invalidates stale assignments, releases a pending JoinGroup barrier without double-incrementing its generation, reelects the leader, and returns v3+ per-member unknown/fenced identity errors |
| DescribeGroups | 0-6 | Classic and offset-only group state, protocol, members, client metadata, assignments, and authorized operations; denied duplicate IDs each retain `GROUP_AUTHORIZATION_FAILED`, denied results precede coordinator results, and authorization/backend failures remain distinct |
| ListGroups | 0-5 | Unified classic/consumer/streams/share/offset-only listing with case-insensitive state and group-type filters; Cluster `DESCRIBE` exposes all matching groups, otherwise entries are filtered independently by Group `DESCRIBE` |
| DeleteGroups | 0-2 | Applies stable distinct-name processing before authorization/mutation, atomically deletes each empty group and its offsets once, preserves per-group errors, and returns `NON_EMPTY_GROUP` for active groups |
| OffsetDelete | 0 | Checks Group `DELETE` and every unique Topic `READ` resource before metadata lookup or deletion, keeps explicit Group/Topic denials distinct from top-level empty authorization-backend errors, leaves all offsets unchanged on those backend failures, deletes committed offsets for inactive subscriptions, and returns `GROUP_SUBSCRIBED_TO_TOPIC` per active topic-partition |
| ConsumerGroupHeartbeat | 0-1 | Kafka 4 consumer protocol membership with group-effective heartbeat/session values constrained by the read-only broker min/max bounds, session expiry, persisted current/previous member epochs with response-loss recovery, owned-partition subset fencing, static epoch `-2` temporary leave/reconnect, PostgreSQL-backed regex resolution with the 10-second metadata guard and configurable maximum refresh interval, and cooperative reconciliation; the ordered `group.consumer.assignors` list chooses the default Rust-native `uniform`/`range` server assignor and fences disabled explicit choices with `UNSUPPORTED_ASSIGNOR`; `group.consumer.max.size` is enforced under the shared coordinator lock and returns `GROUP_MAX_SIZE_REACHED` only for a new member at capacity; an empty classic group converts atomically after stale dynamic expiry, pending classic member IDs are invalidated, and offsets/GROUP config survive, while non-empty migration is not advertised |
| ConsumerGroupDescribe | 0-1 | Group state/epochs, current and target assignments, member metadata, and authorized operations; duplicate denied IDs preserve one error each, and any unauthorized current/target assignment topic replaces the complete group with `TOPIC_AUTHORIZATION_FAILED` and no members |
| StreamsGroupHeartbeat | 0 | `streams.version`-gated Kafka Streams 4.2 broker-driven membership; group heartbeat overrides are bounded to 5000..15000 ms, session overrides to 45000..60000 ms, and standby replicas to 0..`group.streams.max.standby.replicas` (default 2) before persistence, with effective values driving responses and durable liveness; `group.streams.max.size` is enforced under the shared coordinator lock after session expiry and returns `GROUP_MAX_SIZE_REACHED` only for a new member at capacity; stable distinct validation rejects Kafka internal and invalid topology topic names with `STREAMS_INVALID_TOPOLOGY` before topic authorization or mutation; topology/status validation, process/member current/previous epochs with response-loss recovery, owned-task subset fencing, sticky active assignment, stateful standby placement, cooperative ownership transfer, endpoint/task-offset metadata, and session expiry; missing internal topics use Streams factor `0`/effective CreateTopics `-1` broker defaults plus shared topic-config validation/provenance, accept Kafka Streams' range-validated `segment.bytes` repartition hint as a no-op because there are no local log segments, while failed creation remains `MISSING_INTERNAL_TOPICS` without mutation and existing configuration mismatches remain non-blocking |
| StreamsGroupDescribe | 0 | Streams topology readiness, group/assignment epochs, members, current/target task assignments, task offsets, endpoints, authorized operations, duplicate-preserving group ACL results, and complete topology/member removal on topic authorization failure |
| ShareGroupHeartbeat | 1 | PostgreSQL-backed Kafka share-group membership with group-effective heartbeat/session values constrained by the read-only broker min/max bounds, member/session expiry, persisted current/previous epochs with idempotent response-loss recovery, subscriptions, and shared partition assignment; the read-only `group.share.assignors` list contains exactly the implemented Rust-native `simple` assignor; `group.share.max.size` is restricted to 1..1000 and enforced under the shared coordinator lock with `GROUP_MAX_SIZE_REACHED` for a new member at capacity |
| ShareGroupDescribe | 1 | Share-group state, epochs, members, subscriptions, assignments, and authorized operations; duplicate denied IDs preserve one error each, and any unauthorized assignment topic replaces the complete group with `TOPIC_AUTHORIZATION_FAILED` and no members |
| ShareFetch | 1-2 | Stateful share sessions, PostgreSQL record acquisition locks with group-effective duration bounded to 15000..60000 ms, group-effective delivery counts and per-partition lock limits, release/redelivery, exact `share.auto.offset.reset=by_duration:<ISO-8601>` SPSO initialization, v2 `batch_optimized` and strict `record_limit` acquisition, and renew-only request validation; reducing the lock limit blocks new acquisition until existing locks drain, disabled renew returns partition `INVALID_RECORD_STATE`, all current/session topics are Group/Topic-authorized before acknowledgement or acquisition work, topic denial stays per partition, and authorizer failure is a top-level empty `UNKNOWN_SERVER_ERROR` |
| ShareAcknowledge | 1-2 | Atomic accept, release, reject, and gap acknowledgements with session fencing, bounded group-effective acquisition-lock duration, and group-effective final-attempt archival; all topics are Group/Topic-authorized before record mutation, explicit topic denial remains per partition, authorization backend failure is top-level and empty, and v2 adds type-4 lock renewal, `is_renew_ack` validation, the acquisition-lock timeout, and KIP-1240 `INVALID_RECORD_STATE` plus explanation when renew is disabled |
| DescribeShareGroupOffsets | 0-1 | Explicit or all initialized share-partition start offsets, topic IDs, virtual leader epoch, and Group/Describe plus Topic/Describe ACLs; all-topic mode omits denied topic names, explicit denials stay partition-scoped after authorized results, an explicit empty topic list succeeds without probing group state, authorizer failures fail every requested group, and v1 reports KIP-1226 lag from the persisted WriteShareGroupState completion count plus direct acknowledgement overrides, including after Agent reconnect, while preserving unavailable `-1` lag |
| AlterShareGroupOffsets | 0 | Creates or resets an empty share group, clears acquisition state, supports partial unknown-partition results, and enforces batched Group/Read plus Topic/Read ACLs; denied existing topics retain their real topic ID while authorizer failures use the top-level request error |
| DeleteShareGroupOffsets | 0 | Deletes initialized topic state only for an empty share group, supports per-topic results, and enforces batched Group/Delete plus Topic/Read ACLs; denied topics precede coordinator results while authorizer failures use the top-level request error |
| InitializeShareGroupState | 0 | Broker-listener Share Coordinator initialization guarded by Cluster `CLUSTER_ACTION`; atomically replaces PostgreSQL share-partition state without creating an active share group, preserves independent partition errors, and fences an older explicit state epoch |
| ReadShareGroupState | 0 | Reads normalized available/acknowledged/archived offset ranges from the shared coordinator state, applies direct ShareFetch/ShareAcknowledge record overrides, advances the persisted leader epoch, and returns `FENCED_LEADER_EPOCH` for stale readers |
| WriteShareGroupState | 0-1 | Merges range state by delivery-count/state priority without expanding large offset ranges, preserves Kafka state/leader fencing, and makes written terminal ranges immediately unavailable to ShareFetch; v1 persists `delivery_complete_count` |
| DeleteShareGroupState | 0 | Idempotently removes one shared coordinator partition state and its direct acquisition overrides after Cluster `CLUSTER_ACTION`, without requiring or deleting share-group membership |
| ReadShareGroupStateSummary | 0-1 | Returns Kafka's successful uninitialized sentinel or the persisted state epoch, leader epoch, and SPSO without applying request leader-epoch fencing; v1 includes the exact completion count at or above SPSO |
| InitProducerId | 0-6 | Allocates producer IDs and fences prior transactional epochs; requires producer ID and epoch to be both absent or both present, maps fencing to `INVALID_PRODUCER_EPOCH` through v3 and `PRODUCER_FENCED` from v4; non-transactional initialization accepts Cluster `IDEMPOTENT_WRITE` or Kafka-compatible authorization to `WRITE` at least one Topic across wildcard/literal/prefixed ACLs and dominant denies and ignores the timeout field; ordinary transactional initialization enforces the positive `transaction.max.timeout.ms` ceiling; idle Empty/Complete transactional IDs expire after the read-only `transactional.id.expiration.ms` boundary and then receive a new producer ID, while every ongoing transaction remains protected; unstable Kafka 4.2 v6 enables KIP-939 2PC only when the default-disabled `transaction.two.phase.commit.enable` broker gate is set, then requires TransactionalId `WRITE` plus `TWO_PHASE_COMMIT(15)`, exempts that participant from the ordinary timeout ceiling, and returns both the recovery session and preserved transaction identities |
| OffsetForLeaderEpoch | 2-4 | Exposes virtual leader epoch 0 and partition log end while applying Kafka epoch fencing errors; Cluster `CLUSTER_ACTION` bypasses Topic checks, otherwise every unique topic is authorized before metadata reads, backend failures return request-wide per-partition server errors, and authorized topics precede explicit denials |
| AddPartitionsToTxn | 0-5 | v0-v3 require TransactionalId `WRITE`, batch every unique non-internal Topic `WRITE` authorization before metadata work, distinguish ACL denial from request-wide authorizer failure, validate the complete partition set before one atomic registration, return exact failures plus `OPERATION_NOT_ATTEMPTED` for otherwise valid partitions, and down-convert producer fencing through v1; v4-v5 broker requests require only Cluster `CLUSTER_ACTION` and process each transaction, including verify-only, atomically |
| AddOffsetsToTxn | 0-4 | Requires TransactionalId `WRITE` and Group `READ`, registers a consumer group in the active transaction, and maps producer fencing to `INVALID_PRODUCER_EPOCH` through v1 and `PRODUCER_FENCED` from v2 |
| TxnOffsetCommit | 0-5 | Batches TransactionalId/Group/Topic authorization before mutation, validates topic partitions independently, returns per-partition `OFFSET_METADATA_TOO_LARGE` before staging metadata beyond `offset.metadata.max.bytes` using Kafka's Java `String.length()`/UTF-16-code-unit behavior, preserves the supplied committed leader epoch in v2+, stages valid authorized offsets until commit, and performs v5+ transaction-v2 server-side offset-group registration without `AddOffsetsToTxn`; KIP-1251 permits an older consumer member epoch when every requested partition remains assigned from that epoch, but any moved partition returns request-wide `ILLEGAL_GENERATION` and stages no partial offsets |
| EndTxn | 0-5 | Atomically commits/aborts spans and staged consumer offsets, mapping producer fencing to `INVALID_PRODUCER_EPOCH` through v1 and `PRODUCER_FENCED` from v2; v5 bumps the producer epoch exactly once, returns the bumped identity on success/retry, accepts empty aborts, rejects empty commits, and hides producer identity on errors |
| WriteTxnMarkers | 1-2 | Broker-listener marker handling requires Cluster `ALTER` or the Kafka-compatible `CLUSTER_ACTION` fallback and never applies Topic ACLs; valid ordinary partitions and virtual `__consumer_offsets` targets atomically finalize the matching producer transaction only when they cover every registered data partition and publish staged offsets in the same metadata transaction; legacy markers accept equality with the active data epoch, while KIP-1228 v2+ requires a strictly greater epoch, persists the accepted marker epoch, treats the exact completed marker as an idempotent retry, and fences that same epoch when a newer transaction is active; decreasing coordinator epochs return `TRANSACTION_COORDINATOR_FENCED`, producer epoch failures return `INVALID_PRODUCER_EPOCH`, independent unknown partitions retain their own errors, and opposite-result retries return `INVALID_TXN_STATE` |
| DescribeTransactions | 0 | Reports producer, epoch, state, timeout, start time, and active partitions after TransactionalId `DESCRIBE`; participating topics without Topic `DESCRIBE` are omitted, and an authorized empty ID returns `INVALID_REQUEST` |
| ListTransactions | 0-2 | State, producer ID, duration, and full-ID regex filters, with every result independently filtered by TransactionalId `DESCRIBE` and no Cluster-ACL prerequisite; non-empty unknown-only state filters return no transactions while echoing the unknown values, and malformed regexes return `INVALID_REGULAR_EXPRESSION` |
| DescribeProducers | 0 | Reports persisted producer epoch, sequence, timestamp, and current transaction start offset; expired producer state and state without a retained span after retention or `DeleteRecords` are omitted |
| DescribeAcls | 1-3 | PostgreSQL-backed literal/prefixed filters, including ANY, MATCH, and Kafka 4.2 `TWO_PHASE_COMMIT(15)` |
| CreateAcls | 1-3 | Idempotent ACL creation for Kafka resource, operation, host, and permission fields, including operation 15; UNKNOWN/ANY/MATCH construction errors fail every creation before mutation while semantic resource errors remain per-entry |
| DeleteAcls | 1-3 | Per-filter deletion results with the deleted ACL bindings, including operation 15 |
| DescribeUserScramCredentials | 0 | PostgreSQL-backed per-user mechanism and iteration descriptions, including duplicate and missing-user errors |
| AlterUserScramCredentials | 0 | Atomically validates and persists salted SCRAM StoredKey/ServerKey upserts and deletions; new connections observe changes |
| CreateDelegationToken | 1-3 | Creates PostgreSQL-backed HMAC-SHA-512 tokens with owner, requester, exact ordered renewer list, and bounded lifetime; v3 null/empty owner names default to the requester independently of the supplied owner type; cross-owner creation authorizes User `CREATE_TOKENS` against the complete owner principal; broker-side errors preserve owner/requester identities and Kafka timestamp sentinels |
| RenewDelegationToken | 1-2 | Atomically validates owner/requester/renewer and extends expiry without exceeding max lifetime; token-authenticated rejection reports expiry `-1` |
| ExpireDelegationToken | 1-2 | Atomically validates owner/requester/renewer and shortens expiry or immediately removes a token; token-authenticated rejection reports expiry `-1` |
| DescribeDelegationToken | 1-3 | Applies owner filters plus Kafka owner/requester/renewer visibility and DelegationToken/User ACL checks; User `DESCRIBE_TOKENS` uses the complete owner principal |
| DescribeClientQuotas | 0-1 | PostgreSQL-backed `user`, `client-id`, and `ip` quotas with exact/default/specified components, strict matching, and Kafka filter validation |
| AlterClientQuotas | 0-1 | Per-entity validation, validate-only, atomic value upsert/removal, duplicate detection, and Kafka numeric/type constraints |
| SaslHandshake | 0-1 | Negotiates SCRAM-SHA-256 or SCRAM-SHA-512; v0 exchanges length-prefixed opaque SASL tokens before returning to Kafka framing, while v1 uses SaslAuthenticate. Repeated handshakes before authentication are illegal; an authenticated v1 connection may start KIP-368 re-authentication with the original mechanism and principal |
| SaslAuthenticate | 0-2 | Stateful RFC 5802 SCRAM against dynamic credentials, static fallback users, or delegation-token HMAC passwords; KIP-368 same-connection re-authentication requires an unchanged mechanism/principal, token and configured session expiry are enforced, and failed framed authentication returns its error response before connection closure |

Classic `JoinGroup`, `SyncGroup`, `Heartbeat`, and `LeaveGroup`, together with
consumer, streams, and share group heartbeats, return
`GROUP_AUTHORIZATION_FAILED` only for an explicit Group `READ` denial. An
authorization-backend failure is returned as `UNKNOWN_SERVER_ERROR`; request
member IDs, protocol identity, and v3+ LeaveGroup member results remain
schema-correct on both paths.

Kafka 4.3 KIP-1196 broker settings
`group.coordinator.cached.buffer.max.bytes` and
`share.coordinator.cached.buffer.max.bytes` default to `1048588`, enforce
Kafka's `524288` minimum, and support PostgreSQL-backed cluster-wide dynamic
updates with static synonyms. rutomq does not retain coordinator-log append
buffers: `rutomq_coordinator_batch_buffer_cache_bytes` and
`rutomq_coordinator_batch_buffer_cache_discards_total` expose group/share
labels at zero rather than introducing an Agent-local coordinator log.

Kafka 4.3's read-only `add.partitions.to.txn.retry.backoff.ms` and
`add.partitions.to.txn.retry.backoff.max.ms` settings are reported with the
Kafka defaults `20` and `100`. rutomq's AddPartitionsToTxn path serializes the
metadata change directly in PostgreSQL, so it has no asynchronous local
transaction-log append window to retry and these values remain compatibility
metadata.

Kafka 4.3's deprecated `group.coordinator.rebalance.protocols` setting is
reported as the static, read-only LIST value `classic,consumer,streams`.
Protocol availability is governed by `group.version`, `streams.version`, and
`share.version`; share groups remain decoupled from this compatibility setting.

Kafka's read-only `transaction.two.phase.commit.enable` broker setting defaults
to false. Set `RUTOMQ_TRANSACTION_TWO_PHASE_COMMIT_ENABLE=true` on every Agent
to allow KIP-939 enrollment; otherwise `Enable2Pc=true` returns
`TRANSACTIONAL_ID_AUTHORIZATION_FAILED` before producer state is created.
The read-only `transaction.max.timeout.ms` setting defaults to 900000 ms and is
configured by `RUTOMQ_TRANSACTION_MAX_TIMEOUT_MS`. Larger ordinary
transactional requests return `INVALID_TRANSACTION_TIMEOUT` before producer
state is created; idempotent-only and enabled two-phase requests are exempt.

Kafka's cluster-wide dynamic `transaction.partition.verification.enable`
setting defaults to true, with
`RUTOMQ_TRANSACTION_PARTITION_VERIFICATION_ENABLE` as the static fallback.
When disabled, legacy Produce v11 and below may append to an ongoing
transaction without verifying that the target partition was already enrolled.
Producer epoch fencing, transactional-ID ownership, and ongoing transaction
validation are unchanged. Produce v12+ ignores this switch and always uses
transaction-v2 server-side partition registration.

Transactional spans remain `pending` in PostgreSQL until `EndTxn`. A
`read_committed` fetch stops at the first open transaction and hides aborted
spans; `read_uncommitted` returns both. Idle transactions are aborted by a
stateless maintenance worker using the timeout persisted in PostgreSQL.
Object commits that contain multiple producers pre-lock producer rows in
ascending ID order, and the timeout worker uses the same producer-first order
before changing transactions or spans. This prevents cross-Agent Produce and
expiry/EndTxn traffic from leaking a PostgreSQL deadlock as a Kafka server
error. Produce, compaction, and retention use the same topic-name/partition
order whenever one PostgreSQL transaction locks multiple partition rows.
Consumer offset commits validate in request order and then mutate rows in
`(group ID, topic ID, partition)` order. EndTxn publishes staged offsets with
the same SQL order, while OffsetDelete and empty-group deletion acquire that
order before removing rows. Commits protect referenced topics before touching
offset rows, preventing reverse-order OffsetCommit, EndTxn, deletion, and
topic-deletion cycles without changing Kafka response order.
Committed offsets persist their commit time and optional OffsetCommit v2-v4
explicit expiry. A bounded stateless sweep honors explicit deadlines first,
expires standalone/manual commits from commit time, protects active classic,
consumer, and Streams subscriptions, and starts classic group retention when
the group becomes Empty. Pending transactional offset updates prevent removal.
Protected rows advance a fair scan cursor so an old active group cannot starve
later eligible offsets. PostgreSQL advisory and row locks keep concurrent
Agents and commits on the same group-first order. Kafka's read-only
`offset.metadata.max.bytes`, `offsets.retention.minutes`, and
`offsets.retention.check.interval.ms` defaults are 4096, 10080, and 600000.
Transaction v2 clients use Produce v12+ and TxnOffsetCommit v5+ to register
partitions and offset groups on the server, while older request versions still
require the explicit Add APIs. EndTxn v5 commits or aborts and bumps the
producer epoch in one PostgreSQL transaction. Exact retries using the old
identity return the same bumped identity; an empty/current-epoch abort performs
one new bump, while an empty commit returns `INVALID_TXN_STATE`.
Because rutomq has no physical replica logs, `WriteTxnMarkers` maps the
broker-to-broker marker to that same strongly consistent transaction and
object-span state instead of appending local control batches. Global
visibility changes only after all registered data partitions are represented
by valid marker targets. The accepted marker producer/coordinator epochs are
stored with the PostgreSQL transaction. Legacy markers may use the data epoch;
transaction-version 2 markers must use a higher epoch. An exact retry after
completion remains successful, while equality against a newly ongoing
transaction and a decreasing coordinator epoch are fenced before visibility
changes.
`Enable2Pc=true` disables automatic expiry for that transaction.
`KeepPreparedTxn=true` preserves an ongoing transaction across Agent/client
loss, fences the old producer, and returns its original producer ID/epoch
alongside a new recovery session. That recovery session may only commit or
abort the preserved transaction; Produce and transaction-extension requests
are rejected. The pointer, both producer identities, and 2PC state live in
PostgreSQL rather than Agent-local storage.

Time- and size-based retention removes only a contiguous expired prefix of each
partition. Pending transactional spans stop the sweep. Shared objects remain
until every span reference is gone. Each removed span contributes its topic's
persisted `file.delete.delay.ms` deadline (default `60000`, valid `>= 0`) and
the largest deadline wins for an object shared across topics. Physical delete
requires both that deadline and the Agent-wide object safety grace to expire,
so replacement of an Agent cannot shorten an in-flight Fetch safety window.
An eligible committed object row remains in PostgreSQL until OpenDAL deletion
succeeds. Delete failure or Agent replacement therefore repeats the same
idempotent physical delete; a crash after deleting the object but before
metadata completion is recovered the same way, without Agent-local durable
state.
Supported topic keys are
`retention.ms`, `retention.bytes`, `cleanup.policy`, `delete.retention.ms`,
`min.compaction.lag.ms`, `max.compaction.lag.ms`,
`min.cleanable.dirty.ratio`, `min.insync.replicas`, `max.message.bytes`,
`message.timestamp.type`, `message.timestamp.before.max.ms`,
`message.timestamp.after.max.ms`, `compression.type`,
`compression.gzip.level`, `compression.lz4.level`, and
`compression.zstd.level`, `file.delete.delay.ms`, `flush.messages`, and
`flush.ms`.
Every other topic key is rejected by `CreateTopics`, `AlterConfigs`, and
`IncrementalAlterConfigs` with Kafka `INVALID_CONFIG`; representative local
segment/index and tiered-storage keys include `segment.bytes`,
`segment.index.bytes`, `index.interval.bytes`, `remote.storage.enable`, and
`local.retention.ms`. A rejected resource is not partially mutated, unrelated
resources in the same request may still succeed, and validate-only returns the
same error without mutation.
Broker-driven Streams internal-topic creation is the narrow exception for
`segment.bytes`: Kafka Streams emits it for repartition topics, so rutomq
validates Kafka's 1 MiB minimum and 32-bit integer range, then ignores the
local-log-only hint rather than persisting a setting that cannot affect
immutable object packing.
When requested, `DescribeConfigs` returns the effective value first and then
the supported virtual broker default using Kafka's topic-to-broker synonym
names. Untouched topic values return the broker default directly. GROUP and
CLIENT_METRICS resources return their same-name effective synonyms with Kafka
4.3 source IDs 8 and 7 respectively; requests without `include_synonyms` keep
the lists empty. Resource type validation and authorization are completed in
request order before any configuration is read. An invalid type or
authorization-backend failure therefore becomes a request-wide
`INVALID_REQUEST` or `UNKNOWN_SERVER_ERROR`, while ordinary denials remain
resource-level authorization errors. As in Kafka 4.2, successful authorized
resources are emitted first and denied resources are appended afterward,
preserving relative order within each set.
Legacy and incremental config mutations use a separate three-stage path:
resource-local preprocessing, authorization of every remaining resource, then
metadata mutation. An authorizer backend failure therefore cannot leave an
earlier resource partially persisted. Preprocessing errors retain their
request positions while every otherwise valid resource receives
`UNKNOWN_SERVER_ERROR`; ordinary denials remain resource-specific.
Legacy replacement applies to TOPIC, GROUP, and CLIENT_METRICS resources.
GROUP replacement submits SET and DELETE operations for the complete supported
dynamic key set under one metadata update; CLIENT_METRICS does the same inside
its existing advisory-locked transaction. Omitted keys are therefore removed
without an Agent-side read/modify/write race, validate-only leaves persisted
state unchanged, and an empty ClientMetrics replacement removes the
subscription. Kafka 4.2 generated-wire acceptance covers Group and
ClientMetrics because its public Admin interface has removed the deprecated
legacy method; Kafka 3.9 AdminClient separately proves the public legacy
ClientMetrics path.
The two flush settings default to `Long.MAX_VALUE`, so the Agent-wide
250 ms/8 MiB bound remains the default trigger. `flush.messages` counts
admitted records independently per partition, while `flush.ms` contributes the
earliest deadline among partitions in the current cross-topic batch. Reaching
either threshold commits that entire batch through an immutable OpenDAL write
and PostgreSQL metadata transaction before acknowledging Produce; it does not
create a local WAL.
Produce validates each partition before it enters the shared object buffer.
Neither the Produce handler nor the batcher creates topic metadata. After
Topic Write authorization, a missing name or out-of-range partition returns
`UNKNOWN_TOPIC_OR_PARTITION`; an unresolved v13 topic ID returns
`UNKNOWN_TOPIC_ID`. Valid partitions in the same request are still committed.
Metadata remains the only protocol path that can auto-create a topic. Its
partition count comes from the same KIP-1211 virtual-controller default as
CreateTopics `-1`; `auto.create.topics.enable=false` disables that path without
changing explicit Admin creation.
If the connection is lost after that transaction commits but before the
response is written, an idempotent retry on any Agent resolves the producer
ID, epoch, and sequence from PostgreSQL and returns the original base offset.
The retry creates no second visible span or offset. Its redundant immutable
upload remains represented by an uncommitted object intent until orphan GC
removes it, so a response-loss retry cannot leave an untracked S3 object.
The virtual topology has one real service replica and therefore one virtual
ISR member. Required acks outside `0`, `1`, and `-1` return
`INVALID_REQUIRED_ACKS`; `acks=-1` returns `NOT_ENOUGH_REPLICAS` for a topic
whose `min.insync.replicas` exceeds one. `acks=1` and successful `acks=-1`
still wait for immutable object persistence plus the PostgreSQL metadata
transaction. `acks=0` suppresses the response but uses the same asynchronous
durability path. Rejected partitions allocate neither offsets nor objects.
`compression.type=producer` preserves the producer codec; `uncompressed`,
`gzip`, `snappy`, `lz4`, and `zstd` rewrite each batch before upload.
Explicit gzip, LZ4, and Zstandard rewrites apply their topic levels with Kafka
defaults `-1`, `9`, and `3` and valid ranges `-1` or `1..9`, `1..17`, and
`-131072..22`. The levels affect the actual codec output; they are not merely
described config values. Producer-supplied Zstandard is accepted only by
Produce v7 or newer; v3-v6 return
`UNSUPPORTED_COMPRESSION_TYPE(76)`. Those older request versions can still be
broker-recompressed to topic Zstandard when their source codec is supported.
`max.message.bytes` is applied after that final encoding. CreateTime uses the
configured broker-clock windows but accepts `NO_TIMESTAMP(-1)`. A client
LogAppendTime batch is validated against the batch-header `MaxTimestamp`, not
its raw per-record timestamp deltas. An in-range non-sentinel maximum is
accepted and normalized to CreateTime; a sentinel or otherwise non-applicable
client LogAppendTime attribute is rejected. Same-codec normalization preserves
the inner timestamps that become visible under CreateTime, while a topic codec
change rebuilds every record with the interpreted batch maximum. LogAppendTime
topics reject client LogAppendTime batches, while client CreateTime batches
receive one broker timestamp and the LogAppendTime batch bit. Timestamp
rewrites recompute CRC, return the assigned time in Produce when applicable,
and survive Fetch. Before/after window comparisons use Java `long`-equivalent
wrapping subtraction and negation instead of saturating interval endpoints, so
signed 64-bit boundary timestamps follow Kafka 4.2 exactly.
Magic-v2 inner-record timestamp deltas are encoded and decoded as signed
64-bit Varlongs; a Kafka 4.2 Java producer/consumer path verifies an exact
`2147483648` ms same-batch delta through durable Produce and Fetch.
The decoder rejects otherwise valid-CRC magic-v2 data when bytes remain inside
a declared record body or after a positive declared record count. This check
runs before timestamp/compression normalization and durable mutation. It also
rejects a batch record count or record header count larger than the remaining
decoded payload before reserving declaration-sized vector memory. Client
Produce additionally requires the raw magic-v2 `BaseOffset` to be zero before
topic admission; stored broker batches retain their assigned nonzero or sparse
offsets. A record-batch CRC mismatch returns `CORRUPT_MESSAGE(2)`, rather than
the structural `INVALID_RECORD(87)`, before offset allocation, producer-state
mutation, or object creation. Idempotent producer sequences increment modulo
`2^31` in the codec, Agent admission, duplicate history, and both metadata
stores; `Integer.MAX_VALUE` is followed by `0` within one batch or across two
batches. For magic-v2 records, any negative key, value, or header-value length
is decoded as null, matching Kafka's nullable bytes behavior; negative header
key lengths and header counts remain invalid. Decoded
`baseOffset + offsetDelta` uses explicit signed
two's-complement wrapping at `i64` boundaries, matching Java; Produce then
assigns the accepted records their broker-owned partition offsets. A topic
recompression or timestamp rewrite uses the first record as the explicit batch
base and ordered wrapping deltas, so a valid boundary-crossing client batch
cannot overflow before offset assignment. Offset assignment writes virtual
partition leader epoch `0` into every newly appended batch. Record headers are
retained as ordered key/value pairs, so duplicate keys and null values survive
decoding, topic recompression, offset assignment, storage, compaction, and
Fetch. For CreateTime data,
`BaseTimestamp` is the first record timestamp rather than the minimum, while
`MaxTimestamp` remains the maximum; non-monotonic records use signed negative
Varlong deltas. A negative batch base sequence maps every non-idempotent record
to `NO_SEQUENCE(-1)` and remains `-1` after sparse compaction.
Rejected partitions neither allocate offsets nor contribute bytes to an S3
object, while valid partitions in the same request may commit.

Record-key compaction is enabled for `compact` and `compact,delete` topics.
Agents claim expiring PostgreSQL leases, scan only the stable prefix before the
first pending transaction, and write new immutable OpenDAL objects before a
source-span compare-and-swap. Compacted batches carry their original sparse
offsets and valid CRCs; high watermarks and log-start offsets are not
renumbered. New keyless client records are rejected while compaction is
enabled; historical keyless records written before a delete-to-compact policy
change survive cleaning. The latest value per key survives, aborted spans can
be removed, and committed transactional records retain their transaction
metadata. Tombstones are retained until `delete.retention.ms` and schedule a
persisted recheck even when no new records arrive. Old shared
objects become deletable only after their final span reference disappears and
their persisted `file.delete.delay.ms` deadline plus Agent safety floor pass.
The cleaner computes the dirty-to-cleanable ratio from immutable span byte
ranges. It preserves the pending-transaction barrier and minimum lag, waits
until the configured dirty ratio is reached, and overrides only that ratio
when the oldest dirty span reaches `max.compaction.lag.ms`. Tombstone rechecks
remain eligible independently of newly dirty data.
Compaction failures leave new objects invisible for orphan GC, and expired
leases allow another stateless Agent to retry.

Every immutable upload is preceded by a PostgreSQL object intent. Object-span
commit and orphan claim take a row lock on that intent, so a different Agent
cannot collect an upload that is still eligible to commit. A failed metadata
transaction leaves the object invisible; after the configured grace period,
one GC worker marks the intent with `SKIP LOCKED`. That durable claim fences
metadata commit, survives OpenDAL failure or Agent replacement, and is removed
only after idempotent physical deletion succeeds.
Bare objects are reclaimed only when their object-store modification time is
old enough and PostgreSQL has neither a committed row nor an active intent.
Small S3-compatible objects use one conditional OpenDAL PUT. Multipart objects
first reserve the final key with a conditional single PUT and then upload
bounded concurrent parts. This keeps keys immutable across independent Agents
even when a compatible backend ignores conditional headers on multipart
completion. An ambiguous failed upload leaves its reservation invisible and
lets the same intent-aware orphan GC remove it safely. S3 operators use
OpenDAL's bounded exponential retry layer for temporary transport failures;
condition mismatches are not retried and still return `AlreadyExists`
immediately.

The Kafka listener supports TLS server authentication and SCRAM-SHA-256/512.
`RUTOMQ_SASL_ENABLED=true` keeps authentication enabled when all users are
managed dynamically. `AlterUserScramCredentials` stores only salt, iterations,
StoredKey, and ServerKey in PostgreSQL; changed credentials apply to new
connections. The startup JSON credential map remains available as a bootstrap
fallback and credentials are never included in logs. SASL handshake v0 is
supported with Kafka's length-prefixed opaque token exchange; after successful
authentication the same connection returns to normal Kafka request framing.
Handshake v1 continues to use SaslAuthenticate request/response envelopes.
Unsupported negotiation, illegal SASL state, and failed framed authentication
write the versioned Kafka error response before closing the connection.
Authenticated framed connections may start KIP-368 re-authentication with a
new handshake. The mechanism and resulting principal must match the existing
session, credentials and delegation-token state are read again, and the
returned session lifetime is the smaller of token lifetime and
`RUTOMQ_SASL_MAX_REAUTH_MS` when that non-negative setting is nonzero.
`connections.max.reauth.ms` exposes the effective static value through
DescribeConfigs. Ordinary requests close an expired session, while
ApiVersions and the two SASL APIs remain available to complete
re-authentication.

Delegation tokens are enabled by `RUTOMQ_DELEGATION_TOKEN_SECRET`. Their HMAC,
owner, requester, renewers, and timestamps are persisted in PostgreSQL.
SCRAM-SHA-256/512 accepts Kafka's `tokenauth=true` extension and maps the
authenticated principal to the token owner. Management requests require a
regular authenticated principal; owner, requester, and renewers can manage the
token, and lifecycle checks are atomic. Cross-owner User ACL checks retain the
full `PrincipalType:name` resource identity, and renewer arrays retain empty
User names, duplicates, and request order as Kafka does. Broker-side management
errors preserve resolved identities and Kafka's `-1` timestamp sentinel.
CreateDelegationToken v3 treats a null or empty owner name as an omitted owner
and uses the authenticated requester even if the request contains an owner
type.
Expired tokens cannot establish new sessions, and a stateless worker removes
expired rows.

Client quotas are persisted in PostgreSQL and cached by each Agent for at most
one second. Produce and Fetch byte rates use Kafka's eight-level
user/client-id precedence and broker-local shared buckets. Produce responses
carry the computed throttle time; throttled Fetch responses omit records,
unrecord their bandwidth, and grant the retried fetch progress after its
cooldown. Request percentage and controller mutation quotas throttle request
handling, while named/default IP connection-creation quotas are applied before
the Kafka handshake. Quota throttle count and cumulative delay are exported as
Prometheus metrics.
Accepted Kafka requests are also counted by API key and negotiated wire
version, allowing compatibility tests and operators to verify actual protocol
selection.
Each Agent periodically replaces a bounded Prometheus snapshot derived from
shared PostgreSQL metadata. It reports members and state for classic,
consumer, Streams, and share groups, their persisted generation or assignment
epochs, the latest state of each transactional ID, and committed consumer lag
by group/topic/partition. Kafka 4.3 KIP-1257 retention pressure is exported as
`rutomq_partition_retention_size_percent`: currently referenced logical span
bytes divided by effective `retention.bytes` using integer percentages.
Unlimited or zero-byte retention reports zero and delayed cleanup can exceed
100. rutomq does not expose `LocalRetentionSizeInPercent`, because its
stateless Agents have no local log tier. Removed metadata removes stale label
series. Failed collections retain the prior snapshot and increment an error
counter; group, lag, and partition-retention truncation have explicit gauges.
The default 15-second interval, 1,000 groups, 10,000 lag series, and 10,000
partition-retention series are configurable. The gauges describe the same
cluster-wide state on every Agent and must not be summed across replicas.

Cluster feature levels are serialized through a singleton PostgreSQL row.
`metadata.version` supports Kafka 4.0 IV0 through Kafka 4.3 IV0 (levels
22-30). PostgreSQL remains finalized at level 29 while Agents are rolled; an
operator explicitly upgrades to level 30 afterward. Kafka marks level 30 as a
metadata-changing transition, so neither safe nor unsafe downgrade below it is
accepted. `transaction.version` advertises 0-2, and `group.version`,
`share.version`, and `streams.version` advertise 0-1. `ApiVersions` v3 omits
zero-minimum supported features for older-client compatibility and v4 returns
the complete range. Validate-only and failed multi-feature updates do not
change state or epoch; successful updates are atomic across Agents. Level zero
returns `UNSUPPORTED_VERSION` from the corresponding consumer, share, or
Streams group API. `transaction.version` describes coordinator capability and
does not disable transaction APIs. Physical `kraft.version` and
`eligible.leader.replicas.version` remain unadvertised because rutomq does not
implement physical KRaft reconfiguration or ISR/ELR state.

Client telemetry subscriptions are stored as `CLIENT_METRICS` configuration
resources in PostgreSQL. Supported keys are `metrics`, `interval.ms`, and
`match`; intervals are limited to 100-3600000 ms and match attributes use full
regular-expression matches for client ID, client instance ID, software
name/version, source address, and source port. Agents recompute deterministic
subscription IDs from the shared configuration, so a request can be validated
after moving to another Agent. The built-in receiver counts accepted
compressed bytes and pushes but deliberately does not claim to decode or
export OTLP. `RUTOMQ_TELEMETRY_MAX_BYTES` defaults to 1 MiB.

Optional ACL authorization supports Topic, Group, Cluster, TransactionalId,
DelegationToken, and User resource bindings. Existing client APIs enforce the
Kafka authorization mapping for topic, classic group, idempotent producer, and
transaction operations. Literal wildcard and prefixed patterns, host filters,
operation implications, super users, configurable no-ACL behavior, and
explicit deny precedence are supported. `InitProducerId v6` requires
TransactionalId `WRITE` for ordinary participation and, after the broker gate
is enabled, additionally checks the non-implied `TWO_PHASE_COMMIT(15)`
operation when `enable_2_pc` is set.
Read-only enumeration follows Kafka's least-privilege filtering: transaction
listings require TransactionalId `DESCRIBE` per entry, transaction descriptions
remove unauthorized topics, and `ListGroups` falls back from Cluster
`DESCRIBE` to per-Group `DESCRIBE`. Authorization-store failures return server
errors without exposing resource names.

Consumer protocol state and assignments are transactionally persisted in
PostgreSQL. A new target assignment is installed when membership,
subscriptions, assignor choice, or topic metadata changes. Partitions are
revoked from their old owner before becoming available to the new owner.
The default broker-driven heartbeat/session intervals are 5 seconds and
45 seconds.
Consumer, share, and Streams members persist both their current and previous
epochs so an exact retry after a lost successful response returns the current
assignment without advancing state twice. Consumer recovery additionally
requires every reported owned partition to remain in the persisted assignment;
Streams applies the same rule across active, standby, and warmup tasks. A
consumer static member uses epoch `-2` to temporarily leave while retaining its
assignment and disabling expiry, then may reconnect under a fresh member ID
without a spurious group-epoch change.
Kafka 4.3 KIP-1263 assignment batching applies to consumer, share, and Streams
groups. A membership or subscription change advances the group epoch
immediately, but the assignment epoch and target assignment advance only after
the effective assignment interval. The default is `1000 ms`, bounded by
`0..15000 ms`; empty-name BROKER updates change the PostgreSQL-backed
cluster-wide defaults, while `consumer.assignment.interval.ms`,
`share.assignment.interval.ms`, and `streams.assignment.interval.ms` override
individual groups. The assignment timestamp and interval are persisted with
the group, and Streams reports status code 5 with Kafka's initial-delay or
assignment-interval message while computation is deferred. KIP-1263 assignment
offload is enabled by default for all three protocols through
`group.consumer.assignor.offload.enable`,
`group.share.assignor.offload.enable`, and
`group.streams.assignor.offload.enable`; the GROUP-resource forms omit the
`group.` prefix and override one group. Two dedicated workers are configured by
the static `group.coordinator.background.threads` setting. The first heartbeat
of a new group returns epoch 1 with an empty assignment, then a completed
assignment becomes visible at epoch 2. Work is bounded and single-flight per
group within an Agent; PostgreSQL serialization and persisted epoch/timestamp
fencing prevent duplicate or stale publication across Agents. A dropped or
rejected task is rediscovered by a later heartbeat, so no Agent-local queue is
part of durable group state. A share group remains `Reconciling` until every
member acknowledges its published epoch; an exact-current heartbeat collapses
the previous epoch only for that member. Streams status code 5 is emitted only
while `group_epoch > assignment_epoch`, so periodic metadata probes on a
stable group cannot create a false assignment delay.
Kafka 4.3 KIP-1251 additionally persists an assignment epoch for every
consumer partition that is assigned or pending revocation. An OffsetCommit
using the broker-side member epoch retains Kafka's existing ability to commit
any partition. An older member epoch is accepted only when the member still
owns that partition and the request epoch is not older than its assignment
epoch, so one request may commit retained partitions while fencing moved
partitions. PostgreSQL stores these epochs with the group and serializes
validation with offset upserts; a static temporary leave resets retained
epochs to zero before a new member ID takes over. Transactional offset commits
apply the same proof but remain all-or-nothing and translate a stale assignment
to `ILLEGAL_GENERATION`.

Streams group state is serialized under a PostgreSQL advisory group lock.
Joining members publish topology and process metadata; later heartbeats
reconcile active, standby, and warmup task collections as one cooperative
assignment. Active tasks retain their prior owner where balancing permits;
stateful standby tasks are placed on a different process and promoted after
member loss. Regex sources are resolved against shared topic metadata before
ACL authorization. Required changelog and repartition topics are created only
after Topic/Create authorization, with partition counts propagated through
repartition subtopologies. Missing sources, copartition mismatches, stale
topology epochs, and application shutdown are explicit protocol statuses.
Dynamic GROUP settings control heartbeat/session intervals, initial rebalance
delay, and standby count. Expired members are removed before description,
listing, offset validation, and transactional offset commit. Classic,
consumer, Streams, and share protocols cannot reuse the same group ID. The
real Kafka Streams 4.2 acceptance path covers stateless and materialized
topologies, changelog restore after local-state deletion, repartition
topologies, sticky two-process standby placement and promotion, and exact
output without replay under `group.protocol=streams` and `exactly_once_v2`.

Share-group membership, fetch sessions, partition start offsets, acquisition
locks, delivery counts, and acknowledgements are persisted in PostgreSQL.
Agents keep no durable share state locally. Expired members are pruned under
the group advisory lock before descriptions and offset mutations, allowing
administration to recover after a member or Agent disappears. A Kafka 4.2
`KafkaShareConsumer` test verifies two workers on one partition using
`record_limit`, explicit accept/release, redelivery, and duplicate-free
acceptance. ShareFetch v2 preserves producer record-batch boundaries:
`batch_optimized` may exceed `max.poll.records` to return a complete batch,
while `record_limit` is strict. ShareAcknowledge v2 renews locks without
reacquiring records and returns the effective lock timeout. Kafka 4.3 KIP-1240
adds `share.delivery.count.limit`, `share.partition.max.record.locks`, and
`share.renew.acknowledge.enable`. Group updates are checked against the
described broker min/max values; values persisted before a broker-bound change
are capped when evaluated. Lower lock limits drain naturally, and lowering the
delivery limit archives a released final attempt without another delivery.
The existing `share.record.lock.duration.ms` control follows the same model:
its 30000 ms broker default is bounded by read-only 15000 ms and 60000 ms
group limits, and its effective value drives acquisition and renewal responses.
Share offset lag
is `high_watermark - SPSO - terminal_record_count`, or `-1` for an
uninitialized partition. GROUP configuration supports Kafka 4.3's complete
share config set. A `by_duration:<PnDTnHnMn.nS>` reset performs a server-time,
record-level timestamp lookup once, persists the resulting SPSO atomically,
and returns `OFFSET_NOT_AVAILABLE` when Kafka's timestamp lookup has no match.

Classic and Kafka 4 consumer groups share one administrative view. Active
groups cannot be deleted. Empty groups and their committed offsets are removed
in one PostgreSQL transaction. `DescribeGroups` remains the classic-protocol
view; Kafka 4 consumer groups are described through `ConsumerGroupDescribe`.
Classic member session timeouts and heartbeats are persisted in PostgreSQL.
Join, Sync, Heartbeat, and transactional member validation reap expired
members under the group row lock. Membership loss clears assignments, advances
the generation, and deterministically preserves or reelects an active leader.
JoinGroup v4+ persists expiring pending dynamic IDs before returning
`MEMBER_ID_REQUIRED`. Static replacements preserve assignment/generation when
the selected protocol and subscription are unchanged, while old member IDs are
fenced across group and offset APIs. Protocol selection uses the intersection
of every member's ordered protocol set; opaque metadata refreshes do not cause
spurious generation churn. A PostgreSQL rebalance UUID, deadlines, and
per-member joined tokens park JoinGroup responses until the initial delay and
required rejoins complete. Generation assembly is committed once and can be
resumed by any Agent. Polling reaps non-rejoined processes at session timeout,
while rebalance-timeout completion removes dynamic members and retains static
members. SyncGroup and Heartbeat expose `REBALANCE_IN_PROGRESS` while this
barrier is active. LeaveGroup v3+ reports errors per identity tuple and can
release a pending barrier without an extra generation increment.

An unsupported API closes with an error instead of being reported as a
successful Kafka operation.

## Verified client path

`tests/compat/KafkaTransactionSmoke.java` is compiled against the client JARs
from `apache/kafka:4.0.0`. The smoke path covers topic creation, incremental
topic config and describe config, producer initialization, transactional
Produce, `AddOffsetsToTxn`, `TxnOffsetCommit`, commit, abort, transaction
list/describe, topic-id Fetch v13, `read_committed`, and transactional offset
recovery.

`tests/clients/run-orbstack.sh` additionally runs Confluent Python 2.15.0
(bundled librdkafka 2.15.0) and franz-go 1.21.5 against the same diskless
PostgreSQL/MinIO-backed Agent. Each client must pass Admin creation and
metadata, multi-partition idempotent offsets and payloads, group offset
commit/restart without replay, committed and aborted transactions, and both
fetch isolation levels.

`tests/flink/run-orbstack.sh` runs the official Flink 2.2.1 and optionally 2.3.0
runtime with `flink-connector-kafka:5.0.0-2.2` (Kafka client 4.2). It covers a
two-partition exactly-once sink, bounded source, checkpoint offset commits,
source-to-sink transactions, one forced post-checkpoint failure, recovery, and
exact record-set verification. The connector's official parent POM fixes
`kafka-clients` at 4.2.0; the harness does not override that internal binary
contract with Kafka 4.3. The same harness runs two Kafka 4.2 consumers
with `group.protocol=consumer`, verifies server-side cooperative assignment,
both members consuming, duplicate-free delivery, committed offsets, group
description, and restart without replay. Its Kafka 4.2 Admin path also verifies
`DescribeCluster`, filtered `ListGroups`, consumer/classic group types and
states, `DescribeGroups`, empty-group deletion, raw duplicate-name
`DeleteGroups` processing with one mutation/result per distinct group,
raw duplicate-cardinality checks across classic, consumer, streams, and share
group describe schemas,
validate-only and committed partition expansion, Kafka 4.2
`DescribeTopicPartitions` metadata,
authorization-before-existence privacy, invalid-cursor rejection, and
configuration-clamped continuation pages,
`DeleteRecords` low-watermark advancement, offset-out-of-range reset,
committed-offset deletion, active producer state, incremental topic config,
virtual object-store log-directory description, and no-op replica placement.
Raw Streams heartbeats also require exact `STREAMS_INVALID_TOPOLOGY` errors for
Kafka internal and syntactically invalid source/repartition/changelog names
before any group mutation.
It also replaces a classic consumer with the same `group.instance.id`, requires
the old client to receive `FENCED_INSTANCE_ID`, verifies the surviving static
identity through Admin, and removes it through batched LeaveGroup v3+.
The same real Admin/Producer/Consumer path configures a 512-byte topic batch
limit and timestamp windows, accepts an 8 KiB gzip payload by its compressed
batch size, rejects oversized and stale batches with Kafka exceptions, and
verifies exact LogAppendTime metadata and fetched timestamp/type. Generated
wire tests additionally cover the `NO_TIMESTAMP` sentinel, accepted
multi-record LogAppendTime-to-CreateTime normalization by batch
`MaxTimestamp`, preservation of same-codec inner timestamps, and client
LogAppendTime rejection without offset allocation in both configured timestamp
modes. Unit coverage also forces a codec change and verifies that every
rebuilt record receives the interpreted batch maximum. A generated Produce and
Fetch boundary case plus focused unit cases cover Kafka's wrapping timestamp
window arithmetic at `Long.MIN_VALUE`.
It then forces each supported topic compression codec against a gzip producer,
requires exact Java consumer payloads, verifies DescribeConfigs and DELETE
back to `producer`, and rejects a gzip batch whose topic-level uncompressed
form exceeds `max.message.bytes`. The same real Admin/Producer/Consumer path
creates gzip level 9, LZ4 level 17, and Zstandard level 22 topics, verifies
their described values and payloads, rejects an invalid gzip level, and
DELETEs codec levels back to `-1`, `9`, and `3`.
It also creates a topic with `file.delete.delay.ms=123`, describes and
incrementally changes it to `456`, rejects `-1`, and DELETEs the setting back
to Kafka's `60000` ms default. Memory and PostgreSQL tests separately prove
exact deadline boundaries for retention, `DeleteRecords`, topic deletion, and
cross-topic shared objects.
It creates another topic with `flush.messages=7` and `flush.ms=11`, describes
and incrementally changes both values, rejects zero messages and negative
milliseconds, and DELETEs both settings back to `Long.MAX_VALUE`. An
object-store integration test holds the Agent-wide window at one hour and
proves that `flush.messages=1` and `flush.ms=0` each perform the immutable
object write plus PostgreSQL commit within one second.
The same Kafka 4.2 Admin client creates a topic using KIP-1211
virtual-controller partition and replication settings and another through
explicit manual assignments. The acceptance deployment sets the default to
three and requires three and two partitions respectively, virtual broker `0` as the sole
replica, validate-only with no mutation, and typed client exceptions for zero
partitions, replication factor two, assignment gaps, and unknown brokers.
An independent generated Metadata v13 request auto-creates another
three-partition topic, proving the Admin and auto-create paths use one setting.
It also requires `TopicExistsException` from both normal and validate-only
creation of an existing topic, and `InvalidTopicException` for illegal,
overlength, and dot/underscore-colliding names. PostgreSQL tests start
simultaneous colliding creates through independent store handles and require
exactly one winner.
It also verifies preferred leader election and the virtual partition
reassignment lifecycle, plus the single-voter virtual metadata quorum, through
Kafka 4.2 `Admin`. The same client verifies filtered and default
`ListConfigResources` discovery for topics and virtual broker resources, and
requires GROUP discovery to include a dynamically configured group while
excluding an offset-only group.
The same Kafka 4.2 Admin client exercises client-quota validate-only and
committed changes, strict/non-strict/default filters, IP entities, invalid
integral values, removal, and observable Produce/Fetch throttling with recovery.
It also requires the exact logical feature set through Kafka 4.3
`metadata.version` level 30 while level 29 remains finalized before explicit
upgrade, rejects physical KRaft/ELR claims, and verifies feature validate-only, atomic
failure, invalid upgrade direction, committed downgrade, restore, and
monotonic epoch behavior.
The same Kafka 4.2 path runs two `KafkaShareConsumer` members and exercises
`DescribeShareGroups`, `DescribeShareGroupOffsets`,
`AlterShareGroupOffsets`, `DeleteShareGroupOffsets`, and empty share-group
deletion through the real Admin client. A dedicated v2 path verifies strict
record acquisition, lock renewal and re-delivery, lock-timeout propagation,
share lag, and observed ShareFetch/ShareAcknowledge v2 plus
DescribeShareGroupOffsets v1 request counters.
The same harness persists `share.auto.offset.reset=by_duration:PT5M` through
Kafka Admin, produces records with old and recent timestamps, and requires a
real `KafkaShareConsumer` to receive only the recent suffix and persist its
SPSO.
The same Kafka 4.2 client creates, lists, describes, and deletes a
CLIENT_METRICS resource, obtains a broker-assigned client instance ID, and
performs an observed automatic `PushTelemetry` into rutomq's Prometheus
counter.
A live two-member Kafka 4.2 consumer-group path additionally polls Prometheus
before the members leave and requires a stable member count of two, a positive
persisted rebalance epoch, exact zero lag and unlimited-retention percentage
zero for both assigned partitions, and a committed transaction-state gauge.
The same Kafka 4.2 producer creates a delete-only topic with compaction configs,
writes a historical keyless record, enables compaction, continues one
idempotent producer across cleaning, and verifies sparse offsets,
keyless-record rejection, historical keyless-record retention, and tombstone
expiry. It also round-trips dirty-ratio and maximum-lag configs, rejects an
invalid ratio, proves ratio-based suppression before an incremental SET,
proves maximum-lag forcing, and DELETEs both values back to Kafka defaults.
It also creates a topic with `min.insync.replicas=2`, verifies the described
value, rejects zero, requires a real `acks=all` producer to receive
`NOT_ENOUGH_REPLICAS`, rejects raw required acks `2`, proves `acks=1` begins at
offset zero, then DELETEs the config and proves `acks=all` resumes at the next
offset.
The same Kafka 4.2 Admin client negotiates `ListOffsets` v11 and verifies exact
record timestamps, max timestamp, earliest/latest, non-tiered local/remote
selectors, leader epoch, pending-transaction HWM versus LSO, and post-abort LSO
recovery.
The Kafka 4.2 consumer and Admin paths also negotiate OffsetCommit and
OffsetFetch v10. The harness requires their Prometheus API/version counters
before continuing, then verifies topic-ID commits, recovery, Admin offset
mutation, listing, and deletion.
The harness also runs Kafka Streams 4.2 with two stream threads and
`exactly_once_v2` under both `group.protocol=classic` and
`group.protocol=streams`. It verifies exact key-preserving output, closes the
application, deletes its local state, and restarts the same application ID
without replay. The Streams-protocol path additionally requires a stable
two-member `StreamsGroupDescribe`, type-filtered `ListGroups`, materialized
changelog restore, repartition-topic partition propagation, dynamic GROUP
config discovery, two-process standby placement/promotion, and observed API
88/89 v0 traffic.
Final checkpoint offsets are polled for the asynchronous Flink commit. If a
bounded job finishes before its final monitoring commit, the verifier resumes
from the persisted checkpoint and requires the exact remaining suffix without
duplicate offsets.

`tests/security/run-orbstack.sh` runs Kafka client 4.3 over `SASL_SSL`. It
creates, describes, authenticates, and deletes PostgreSQL-backed SCRAM
credentials through Kafka Admin, and verifies certificate hostname validation,
both SCRAM mechanisms, ACL create/describe/delete, delegation-token
create/renew/describe/expire and token-SCRAM Produce/Fetch, cross-owner
requester lifecycle eligibility, v3 null/empty owner-name defaulting,
full-principal User token ACLs, exact renewer round trips, generated-wire
management error fields, authenticated authorization rejection,
invalid-password and expired-token rejection, and authentication metrics. It
also requires the client to negotiate `DescribeLogDirs` v5 exactly, return
the stateless virtual object-store directory with `is_cordoned=false`, and
round-trip all three KIP-1240 GROUP controls while rejecting an out-of-bounds
delivery limit. It also round-trips all three KIP-1263 cluster-wide BROKER
defaults and GROUP overrides with exact Kafka config sources and synonyms,
rejects a value beyond the broker bounds, and restores every value through
DELETE. KIP-1196 additionally round-trips both coordinator cached-buffer
limits, rejects `524287` atomically, and restores their static defaults. It
also holds mixed visible/hidden transactions open while a least-privilege
principal verifies transactional-ID, participating topic, and group-list
filtering before and after Cluster `DESCRIBE`.

`tests/multi-agent/run-orbstack.sh` runs three mount-free Agents behind one
HAProxy endpoint. It deterministically drops the first idempotent Produce
connection after object and PostgreSQL commit, removes that Agent, and retries
the same producer identity and sequence through a second Agent. It requires
the original offset, one visible span, and one physical object after GC. The
same gate also verifies concurrent idempotent writes through Agent SIGKILL and
retry, two complete Agent restarts, S3 unavailability, a deterministic
PostgreSQL failure after object upload, orphan reclamation, and an exact
committed-row-to-MinIO-object count with format-v1 checksums on every live
span.

`tests/storage/run-orbstack.sh` uses real MinIO to write an object larger than
two 5 MiB parts, requires a multipart ETag, verifies a range crossing a part
boundary, rejects replacement from an independent store instance, verifies
both Agents can immediately range-read after the conditional conflict through
bounded temporary-error retry, and Fetches all checksum-verified partition
ranges from one 10+ MiB Kafka object.

`tests/kubernetes/run-orbstack.sh` validates the production Helm chart on the
OrbStack Kubernetes context. A Kafka 4.2 client writes through the stable
Service during a zero-unavailable image rollout that replaces every original
Agent, then during scale-out from three to five replicas and scale-in to two.
The latest isolated run applied migration 30, retained 1,600 unique records at
contiguous offsets, used no Agent PVC or data volume, and left no staged upload
intent. SIGTERM
marks readiness false and drains queued Produce requests before the configured
grace deadline.

`tests/performance/run-orbstack.sh` records a fixed-resource one/three-Agent
baseline across 1/8 MiB batch bounds and 25/250 ms flush windows. It captures
Produce p50/p95/p99, throughput, OpenDAL requests/bytes/latency, PostgreSQL
metadata-commit latency, and Fetch-cache behavior. Every case re-reads and
validates all unique records, producing equal first-read misses and second-read
hits with zero eviction in the initial run. The checked-in numbers are point
measurements, not capacity promises.
