CREATE TABLE IF NOT EXISTS group_configs (
    group_id TEXT NOT NULL CHECK (group_id <> ''),
    config_key TEXT NOT NULL CHECK (config_key <> ''),
    config_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, config_key)
);

ALTER TABLE streams_protocol_groups
    ADD COLUMN IF NOT EXISTS num_standby_replicas INTEGER NOT NULL DEFAULT 0
        CHECK (num_standby_replicas >= 0),
    ADD COLUMN IF NOT EXISTS initial_rebalance_deadline TIMESTAMPTZ;
