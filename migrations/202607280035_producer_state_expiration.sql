DROP INDEX IF EXISTS object_spans_idempotence_idx;

CREATE INDEX IF NOT EXISTS object_spans_producer_sequence_idx
    ON object_spans (
        topic_id,
        partition_index,
        producer_id,
        producer_epoch,
        first_sequence,
        base_offset DESC
    )
    WHERE producer_id IS NOT NULL;
