ALTER TABLE consumer_protocol_members
    ADD COLUMN IF NOT EXISTS previous_member_epoch INTEGER NOT NULL DEFAULT 0;

ALTER TABLE share_group_members
    ADD COLUMN IF NOT EXISTS previous_member_epoch INTEGER NOT NULL DEFAULT 0;

ALTER TABLE streams_protocol_members
    ADD COLUMN IF NOT EXISTS previous_member_epoch INTEGER NOT NULL DEFAULT 0;
