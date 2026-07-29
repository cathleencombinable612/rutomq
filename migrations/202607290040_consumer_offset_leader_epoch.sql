ALTER TABLE consumer_offsets
    ADD COLUMN IF NOT EXISTS committed_leader_epoch INTEGER NOT NULL DEFAULT -1;

ALTER TABLE transaction_offset_commits
    ADD COLUMN IF NOT EXISTS committed_leader_epoch INTEGER NOT NULL DEFAULT -1;
