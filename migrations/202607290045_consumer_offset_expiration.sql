ALTER TABLE consumer_offsets
    ADD COLUMN IF NOT EXISTS commit_timestamp_ms BIGINT,
    ADD COLUMN IF NOT EXISTS expire_timestamp_ms BIGINT,
    ADD COLUMN IF NOT EXISTS expiration_checked_at_ms BIGINT NOT NULL DEFAULT 0;

UPDATE consumer_offsets
SET commit_timestamp_ms =
    FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT
WHERE commit_timestamp_ms IS NULL;

ALTER TABLE consumer_offsets
    ALTER COLUMN commit_timestamp_ms SET NOT NULL,
    ALTER COLUMN commit_timestamp_ms
        SET DEFAULT FLOOR(EXTRACT(EPOCH FROM now()) * 1000)::BIGINT;

CREATE INDEX IF NOT EXISTS consumer_offsets_expiration_idx
    ON consumer_offsets (
        expiration_checked_at_ms,
        commit_timestamp_ms,
        expire_timestamp_ms
    );

ALTER TABLE transaction_offset_commits
    ADD COLUMN IF NOT EXISTS commit_timestamp_ms BIGINT,
    ADD COLUMN IF NOT EXISTS expire_timestamp_ms BIGINT;

UPDATE transaction_offset_commits
SET commit_timestamp_ms = FLOOR(EXTRACT(EPOCH FROM now()) * 1000)::BIGINT
WHERE commit_timestamp_ms IS NULL;

ALTER TABLE transaction_offset_commits
    ALTER COLUMN commit_timestamp_ms SET NOT NULL,
    ALTER COLUMN commit_timestamp_ms
        SET DEFAULT FLOOR(EXTRACT(EPOCH FROM now()) * 1000)::BIGINT;

ALTER TABLE consumer_groups
    ADD COLUMN IF NOT EXISTS empty_since_ms BIGINT;

UPDATE consumer_groups AS group_state
SET empty_since_ms =
    FLOOR(EXTRACT(EPOCH FROM group_state.updated_at) * 1000)::BIGINT
WHERE empty_since_ms IS NULL
  AND NOT EXISTS (
      SELECT 1
      FROM consumer_group_members AS member
      WHERE member.group_id = group_state.group_id
  );
