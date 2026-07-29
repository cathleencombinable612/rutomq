CREATE TABLE IF NOT EXISTS streams_protocol_groups (
    group_id TEXT PRIMARY KEY,
    group_epoch INTEGER NOT NULL DEFAULT 0,
    assignment_epoch INTEGER NOT NULL DEFAULT 0,
    endpoint_information_epoch INTEGER NOT NULL DEFAULT 0,
    topology JSONB NOT NULL,
    statuses JSONB NOT NULL DEFAULT '[]'::jsonb,
    shutdown_requested BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS streams_protocol_members (
    group_id TEXT NOT NULL
        REFERENCES streams_protocol_groups(group_id) ON DELETE CASCADE,
    member_id TEXT NOT NULL,
    member_epoch INTEGER NOT NULL DEFAULT 0,
    instance_id TEXT,
    rack_id TEXT,
    rebalance_timeout_ms INTEGER NOT NULL,
    session_timeout_ms INTEGER NOT NULL CHECK (session_timeout_ms > 0),
    topology_epoch INTEGER NOT NULL,
    process_id TEXT NOT NULL,
    user_endpoint JSONB,
    client_tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    task_offsets JSONB NOT NULL DEFAULT '[]'::jsonb,
    task_end_offsets JSONB NOT NULL DEFAULT '[]'::jsonb,
    current_assignment JSONB NOT NULL,
    target_assignment JSONB NOT NULL,
    owned_assignment JSONB NOT NULL,
    client_id TEXT NOT NULL DEFAULT '',
    client_host TEXT NOT NULL DEFAULT '',
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, member_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS streams_protocol_member_instance_idx
    ON streams_protocol_members (group_id, instance_id)
    WHERE instance_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS streams_protocol_member_heartbeat_idx
    ON streams_protocol_members (last_heartbeat);
