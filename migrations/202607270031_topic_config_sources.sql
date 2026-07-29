ALTER TABLE topic_configs
    ADD COLUMN IF NOT EXISTS dynamic_config_names TEXT[] NOT NULL
        DEFAULT ARRAY[]::TEXT[];

UPDATE topic_configs
SET dynamic_config_names = ARRAY(
    SELECT name
    FROM (
        VALUES
            ('retention.ms', retention_ms IS DISTINCT FROM 604800000),
            ('retention.bytes', retention_bytes IS DISTINCT FROM -1),
            ('cleanup.policy', cleanup_policy IS DISTINCT FROM 'delete'),
            ('file.delete.delay.ms', file_delete_delay_ms IS DISTINCT FROM 60000),
            ('flush.messages', flush_messages IS DISTINCT FROM 9223372036854775807),
            ('flush.ms', flush_ms IS DISTINCT FROM 9223372036854775807),
            ('delete.retention.ms', delete_retention_ms IS DISTINCT FROM 86400000),
            ('min.compaction.lag.ms', min_compaction_lag_ms IS DISTINCT FROM 0),
            ('max.compaction.lag.ms', max_compaction_lag_ms IS DISTINCT FROM 9223372036854775807),
            ('min.cleanable.dirty.ratio', min_cleanable_dirty_ratio IS DISTINCT FROM 0.5),
            ('min.insync.replicas', min_insync_replicas IS DISTINCT FROM 1),
            ('max.message.bytes', max_message_bytes IS DISTINCT FROM 1048588),
            ('compression.type', compression_type IS DISTINCT FROM 'producer'),
            ('compression.gzip.level', compression_gzip_level IS DISTINCT FROM -1),
            ('compression.lz4.level', compression_lz4_level IS DISTINCT FROM 9),
            ('compression.zstd.level', compression_zstd_level IS DISTINCT FROM 3),
            ('message.timestamp.type', message_timestamp_type IS DISTINCT FROM 'CreateTime'),
            ('message.timestamp.before.max.ms', message_timestamp_before_max_ms IS DISTINCT FROM 9223372036854775807),
            ('message.timestamp.after.max.ms', message_timestamp_after_max_ms IS DISTINCT FROM 3600000)
    ) AS configs(name, changed)
    WHERE changed
)
WHERE cardinality(dynamic_config_names) = 0;
