ALTER TABLE producer_sequences
    ADD COLUMN IF NOT EXISTS history_start_offset BIGINT;

UPDATE producer_sequences ps
SET history_start_offset = COALESCE(
    (
        SELECT MIN(recent.base_offset)
        FROM (
            SELECT os.base_offset
            FROM object_spans os
            WHERE os.producer_id = ps.producer_id
              AND os.topic_id = ps.topic_id
              AND os.partition_index = ps.partition_index
              AND os.producer_epoch = ps.producer_epoch
            ORDER BY os.base_offset DESC
            LIMIT 5
        ) recent
    ),
    ps.last_offset
)
WHERE ps.history_start_offset IS NULL;

ALTER TABLE producer_sequences
    ALTER COLUMN history_start_offset SET NOT NULL;

DROP INDEX IF EXISTS object_spans_producer_sequence_idx;

CREATE INDEX object_spans_producer_sequence_idx
    ON object_spans (
        topic_id,
        partition_index,
        producer_id,
        producer_epoch,
        base_offset DESC
    )
    WHERE producer_id IS NOT NULL;
