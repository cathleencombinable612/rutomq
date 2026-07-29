ALTER TABLE topic_configs
    ADD COLUMN IF NOT EXISTS compression_gzip_level INTEGER NOT NULL DEFAULT -1,
    ADD COLUMN IF NOT EXISTS compression_lz4_level INTEGER NOT NULL DEFAULT 9,
    ADD COLUMN IF NOT EXISTS compression_zstd_level INTEGER NOT NULL DEFAULT 3;

ALTER TABLE topic_configs
    DROP CONSTRAINT IF EXISTS topic_configs_compression_gzip_level_check,
    DROP CONSTRAINT IF EXISTS topic_configs_compression_lz4_level_check,
    DROP CONSTRAINT IF EXISTS topic_configs_compression_zstd_level_check;

ALTER TABLE topic_configs
    ADD CONSTRAINT topic_configs_compression_gzip_level_check
        CHECK (
            compression_gzip_level = -1
            OR compression_gzip_level BETWEEN 1 AND 9
        ),
    ADD CONSTRAINT topic_configs_compression_lz4_level_check
        CHECK (compression_lz4_level BETWEEN 1 AND 17),
    ADD CONSTRAINT topic_configs_compression_zstd_level_check
        CHECK (compression_zstd_level BETWEEN -131072 AND 22);
