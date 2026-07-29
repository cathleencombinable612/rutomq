ALTER TABLE topic_configs
    ADD COLUMN flush_messages BIGINT NOT NULL DEFAULT 9223372036854775807
        CHECK (flush_messages >= 1),
    ADD COLUMN flush_ms BIGINT NOT NULL DEFAULT 9223372036854775807
        CHECK (flush_ms >= 0);
