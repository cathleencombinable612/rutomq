ALTER TABLE consumer_protocol_groups
    ALTER COLUMN group_epoch SET DEFAULT 1,
    ALTER COLUMN assignment_epoch SET DEFAULT 1,
    ADD COLUMN IF NOT EXISTS assignment_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS assignment_interval_ms INTEGER NOT NULL DEFAULT 1000
        CHECK (assignment_interval_ms >= 0);

ALTER TABLE share_groups
    ALTER COLUMN group_epoch SET DEFAULT 1,
    ALTER COLUMN assignment_epoch SET DEFAULT 1,
    ADD COLUMN IF NOT EXISTS assignment_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS assignment_interval_ms INTEGER NOT NULL DEFAULT 1000
        CHECK (assignment_interval_ms >= 0);

ALTER TABLE streams_protocol_groups
    ALTER COLUMN group_epoch SET DEFAULT 1,
    ALTER COLUMN assignment_epoch SET DEFAULT 1,
    ADD COLUMN IF NOT EXISTS assignment_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS assignment_interval_ms INTEGER NOT NULL DEFAULT 1000
        CHECK (assignment_interval_ms >= 0);

UPDATE consumer_protocol_groups
SET group_epoch = 1, assignment_epoch = 1
WHERE group_epoch = 0 AND assignment_epoch = 0;

UPDATE share_groups
SET group_epoch = 1, assignment_epoch = 1
WHERE group_epoch = 0 AND assignment_epoch = 0;

UPDATE streams_protocol_groups
SET group_epoch = 1, assignment_epoch = 1
WHERE group_epoch = 0 AND assignment_epoch = 0;
