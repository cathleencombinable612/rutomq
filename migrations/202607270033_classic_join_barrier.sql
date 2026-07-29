ALTER TABLE consumer_groups
    ADD COLUMN IF NOT EXISTS classic_rebalance_id UUID,
    ADD COLUMN IF NOT EXISTS classic_rebalance_pending BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS classic_rebalance_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS classic_rebalance_deadline TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS classic_initial_rebalance_deadline TIMESTAMPTZ;

ALTER TABLE consumer_group_members
    ADD COLUMN IF NOT EXISTS rebalance_timeout_ms INTEGER NOT NULL DEFAULT 300000
        CHECK (rebalance_timeout_ms > 0),
    ADD COLUMN IF NOT EXISTS classic_joined_rebalance_id UUID;
