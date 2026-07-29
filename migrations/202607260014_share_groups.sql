CREATE TABLE share_groups (
    group_id TEXT PRIMARY KEY,
    group_epoch INTEGER NOT NULL DEFAULT 0,
    assignment_epoch INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT share_groups_id_not_empty CHECK (group_id <> '')
);

CREATE TABLE share_group_members (
    group_id TEXT NOT NULL REFERENCES share_groups(group_id) ON DELETE CASCADE,
    member_id TEXT NOT NULL,
    rack_id TEXT,
    member_epoch INTEGER NOT NULL,
    session_timeout_ms INTEGER NOT NULL CHECK (session_timeout_ms > 0),
    subscribed_topic_names TEXT[] NOT NULL,
    client_id TEXT NOT NULL DEFAULT '',
    client_host TEXT NOT NULL DEFAULT '',
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, member_id)
);

CREATE INDEX share_group_members_heartbeat_idx
    ON share_group_members (last_heartbeat);

CREATE TABLE share_group_assignments (
    group_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partitions INTEGER[] NOT NULL,
    PRIMARY KEY (group_id, member_id, topic_id),
    FOREIGN KEY (group_id, member_id)
        REFERENCES share_group_members(group_id, member_id) ON DELETE CASCADE
);

CREATE TABLE share_group_configs (
    group_id TEXT NOT NULL,
    config_key TEXT NOT NULL,
    config_value TEXT NOT NULL,
    PRIMARY KEY (group_id, config_key)
);

CREATE TABLE share_partition_states (
    group_id TEXT NOT NULL REFERENCES share_groups(group_id) ON DELETE CASCADE,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partition_index INTEGER NOT NULL,
    start_offset BIGINT NOT NULL,
    state_epoch INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (group_id, topic_id, partition_index)
);

CREATE TABLE share_record_states (
    group_id TEXT NOT NULL,
    topic_id UUID NOT NULL,
    partition_index INTEGER NOT NULL,
    record_offset BIGINT NOT NULL,
    record_state TEXT NOT NULL
        CHECK (record_state IN ('available', 'acquired', 'archived')),
    member_id TEXT,
    delivery_count SMALLINT NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
    lock_expires_at TIMESTAMPTZ,
    PRIMARY KEY (group_id, topic_id, partition_index, record_offset),
    FOREIGN KEY (group_id, topic_id, partition_index)
        REFERENCES share_partition_states(group_id, topic_id, partition_index)
        ON DELETE CASCADE
);

CREATE INDEX share_record_states_expiry_idx
    ON share_record_states (lock_expires_at)
    WHERE record_state = 'acquired';

CREATE TABLE share_fetch_sessions (
    group_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    session_epoch INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (group_id, member_id),
    FOREIGN KEY (group_id, member_id)
        REFERENCES share_group_members(group_id, member_id) ON DELETE CASCADE
);

CREATE TABLE share_fetch_session_partitions (
    group_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    topic_id UUID NOT NULL,
    partition_index INTEGER NOT NULL,
    partition_max_bytes INTEGER NOT NULL,
    PRIMARY KEY (group_id, member_id, topic_id, partition_index),
    FOREIGN KEY (group_id, member_id)
        REFERENCES share_fetch_sessions(group_id, member_id) ON DELETE CASCADE,
    FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
);
