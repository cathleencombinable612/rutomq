ALTER TABLE consumer_group_members
    ADD COLUMN IF NOT EXISTS protocol_names TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS protocol_metadata_set BYTEA[] NOT NULL DEFAULT '{}';

UPDATE consumer_group_members
SET protocol_names = ARRAY[protocol_name],
    protocol_metadata_set = ARRAY[protocol_metadata]::BYTEA[]
WHERE cardinality(protocol_names) = 0;

CREATE UNIQUE INDEX IF NOT EXISTS consumer_group_members_instance_idx
    ON consumer_group_members (group_id, group_instance_id)
    WHERE group_instance_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS classic_group_pending_members (
    group_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (group_id, member_id)
);

CREATE INDEX IF NOT EXISTS classic_group_pending_members_expiry_idx
    ON classic_group_pending_members (expires_at);
