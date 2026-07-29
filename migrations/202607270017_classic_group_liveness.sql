ALTER TABLE consumer_group_members
    ADD COLUMN IF NOT EXISTS session_timeout_ms INTEGER NOT NULL DEFAULT 45000
        CHECK (session_timeout_ms > 0);

CREATE INDEX IF NOT EXISTS consumer_group_members_heartbeat_idx
    ON consumer_group_members (last_heartbeat);
