ALTER TABLE topic_configs
    ADD COLUMN IF NOT EXISTS file_delete_delay_ms BIGINT NOT NULL DEFAULT 60000;

ALTER TABLE topic_configs
    DROP CONSTRAINT IF EXISTS topic_configs_file_delete_delay_ms_check;

ALTER TABLE topic_configs
    ADD CONSTRAINT topic_configs_file_delete_delay_ms_check
        CHECK (file_delete_delay_ms >= 0);

ALTER TABLE objects
    ADD COLUMN IF NOT EXISTS delete_after TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS objects_delete_after_idx
    ON objects (delete_after)
    WHERE delete_after IS NOT NULL;
