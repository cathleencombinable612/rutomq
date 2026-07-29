ALTER TABLE topic_configs
    ADD COLUMN IF NOT EXISTS min_insync_replicas INTEGER NOT NULL DEFAULT 1;

ALTER TABLE topic_configs
    DROP CONSTRAINT IF EXISTS topic_configs_min_insync_replicas_check;

ALTER TABLE topic_configs
    ADD CONSTRAINT topic_configs_min_insync_replicas_check
        CHECK (min_insync_replicas >= 1);
