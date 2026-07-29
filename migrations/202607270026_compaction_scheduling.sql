ALTER TABLE topic_configs
    ADD COLUMN IF NOT EXISTS max_compaction_lag_ms BIGINT NOT NULL
        DEFAULT 9223372036854775807,
    ADD COLUMN IF NOT EXISTS min_cleanable_dirty_ratio DOUBLE PRECISION NOT NULL
        DEFAULT 0.5;

ALTER TABLE topic_configs
    DROP CONSTRAINT IF EXISTS topic_configs_compaction_lag_check,
    DROP CONSTRAINT IF EXISTS topic_configs_cleanable_dirty_ratio_check;

ALTER TABLE topic_configs
    ADD CONSTRAINT topic_configs_compaction_lag_check CHECK (
        max_compaction_lag_ms >= 1
        AND max_compaction_lag_ms >= min_compaction_lag_ms
    ),
    ADD CONSTRAINT topic_configs_cleanable_dirty_ratio_check CHECK (
        min_cleanable_dirty_ratio >= 0
        AND min_cleanable_dirty_ratio <= 1
    );
