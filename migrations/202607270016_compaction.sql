ALTER TABLE object_spans
    ADD COLUMN IF NOT EXISTS offsets_preserved BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE partitions
    ADD COLUMN IF NOT EXISTS compaction_last_offset BIGINT NOT NULL DEFAULT -1,
    ADD COLUMN IF NOT EXISTS compaction_recheck_at_ms BIGINT,
    ADD COLUMN IF NOT EXISTS compaction_lease_id UUID,
    ADD COLUMN IF NOT EXISTS compaction_lease_until_ms BIGINT;

CREATE INDEX IF NOT EXISTS partitions_compaction_lease_idx
    ON partitions (compaction_lease_until_ms)
    WHERE compaction_lease_id IS NOT NULL;
