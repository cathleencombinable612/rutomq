ALTER TABLE topic_configs
    ADD COLUMN compression_type TEXT NOT NULL DEFAULT 'producer'
        CHECK (
            compression_type IN (
                'producer',
                'uncompressed',
                'gzip',
                'snappy',
                'lz4',
                'zstd'
            )
        );
