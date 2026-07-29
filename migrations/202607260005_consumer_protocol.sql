CREATE TABLE IF NOT EXISTS consumer_protocol_groups (
    group_id TEXT PRIMARY KEY,
    group_epoch INTEGER NOT NULL DEFAULT 0,
    assignment_epoch INTEGER NOT NULL DEFAULT 0,
    assignor_name TEXT NOT NULL DEFAULT 'uniform',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS consumer_protocol_members (
    group_id TEXT NOT NULL
        REFERENCES consumer_protocol_groups(group_id) ON DELETE CASCADE,
    member_id TEXT NOT NULL,
    instance_id TEXT,
    rack_id TEXT,
    member_epoch INTEGER NOT NULL DEFAULT 0,
    rebalance_timeout_ms INTEGER NOT NULL,
    session_timeout_ms INTEGER NOT NULL CHECK (session_timeout_ms > 0),
    subscribed_topic_names TEXT[] NOT NULL DEFAULT '{}',
    subscribed_topic_regex TEXT,
    client_id TEXT NOT NULL DEFAULT '',
    client_host TEXT NOT NULL DEFAULT '',
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, member_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS consumer_protocol_member_instance_idx
    ON consumer_protocol_members (group_id, instance_id)
    WHERE instance_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS consumer_protocol_member_heartbeat_idx
    ON consumer_protocol_members (last_heartbeat);

CREATE TABLE IF NOT EXISTS consumer_protocol_assignments (
    group_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    assignment_kind TEXT NOT NULL
        CHECK (assignment_kind IN ('current', 'target', 'owned')),
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partitions INTEGER[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (group_id, member_id, assignment_kind, topic_id),
    FOREIGN KEY (group_id, member_id)
        REFERENCES consumer_protocol_members(group_id, member_id)
        ON DELETE CASCADE
);
