ALTER TABLE topic_configs
    ADD COLUMN max_message_bytes INTEGER NOT NULL DEFAULT 1048588
        CHECK (max_message_bytes >= 0),
    ADD COLUMN message_timestamp_type TEXT NOT NULL DEFAULT 'CreateTime'
        CHECK (message_timestamp_type IN ('CreateTime', 'LogAppendTime')),
    ADD COLUMN message_timestamp_before_max_ms BIGINT NOT NULL DEFAULT 9223372036854775807
        CHECK (message_timestamp_before_max_ms >= 0),
    ADD COLUMN message_timestamp_after_max_ms BIGINT NOT NULL DEFAULT 3600000
        CHECK (message_timestamp_after_max_ms >= 0);
