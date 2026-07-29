CREATE TABLE IF NOT EXISTS topics (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    partition_count INTEGER NOT NULL CHECK (partition_count > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS partitions (
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partition_index INTEGER NOT NULL CHECK (partition_index >= 0),
    next_offset BIGINT NOT NULL DEFAULT 0,
    log_start_offset BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (topic_id, partition_index)
);

CREATE TABLE IF NOT EXISTS objects (
    object_key TEXT PRIMARY KEY,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS object_spans (
    id BIGSERIAL PRIMARY KEY,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partition_index INTEGER NOT NULL,
    object_key TEXT NOT NULL REFERENCES objects(object_key) ON DELETE RESTRICT,
    byte_start BIGINT NOT NULL CHECK (byte_start >= 0),
    byte_end BIGINT NOT NULL CHECK (byte_end >= byte_start),
    base_offset BIGINT NOT NULL,
    last_offset BIGINT NOT NULL,
    record_count INTEGER NOT NULL CHECK (record_count > 0),
    timestamp_ms BIGINT NOT NULL,
    txn_state TEXT NOT NULL DEFAULT 'visible',
    UNIQUE (topic_id, partition_index, base_offset)
);

CREATE INDEX IF NOT EXISTS object_spans_fetch_idx
    ON object_spans (topic_id, partition_index, base_offset);

CREATE TABLE IF NOT EXISTS consumer_offsets (
    group_id TEXT NOT NULL,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partition_index INTEGER NOT NULL,
    committed_offset BIGINT NOT NULL,
    metadata TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, topic_id, partition_index)
);

CREATE TABLE IF NOT EXISTS acl_rules (
    id BIGSERIAL PRIMARY KEY,
    principal TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    permission TEXT NOT NULL,
    pattern_type TEXT NOT NULL DEFAULT 'literal'
);

CREATE TABLE IF NOT EXISTS consumer_groups (
    group_id TEXT PRIMARY KEY,
    generation_id INTEGER NOT NULL DEFAULT 0,
    protocol_type TEXT NOT NULL,
    protocol_name TEXT,
    leader_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS consumer_group_members (
    group_id TEXT NOT NULL REFERENCES consumer_groups(group_id) ON DELETE CASCADE,
    member_id TEXT NOT NULL,
    group_instance_id TEXT,
    protocol_name TEXT NOT NULL,
    protocol_metadata BYTEA NOT NULL,
    assignment BYTEA,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, member_id)
);
