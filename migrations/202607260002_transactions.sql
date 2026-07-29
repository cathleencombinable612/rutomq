CREATE SEQUENCE IF NOT EXISTS producer_id_sequence AS BIGINT START WITH 1;

CREATE TABLE IF NOT EXISTS producers (
    producer_id BIGINT PRIMARY KEY DEFAULT nextval('producer_id_sequence'),
    producer_epoch SMALLINT NOT NULL DEFAULT 0 CHECK (producer_epoch >= 0),
    transactional_id TEXT UNIQUE,
    transaction_timeout_ms INTEGER NOT NULL DEFAULT 60000
        CHECK (transaction_timeout_ms > 0),
    current_transaction_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY,
    transactional_id TEXT NOT NULL,
    producer_id BIGINT NOT NULL REFERENCES producers(producer_id),
    producer_epoch SMALLINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ongoing', 'committed', 'aborted')),
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms > 0),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

ALTER TABLE producers
    ADD CONSTRAINT producers_current_transaction_fk
    FOREIGN KEY (current_transaction_id) REFERENCES transactions(id);

CREATE UNIQUE INDEX IF NOT EXISTS transactions_one_ongoing_idx
    ON transactions (transactional_id)
    WHERE status = 'ongoing';

CREATE TABLE IF NOT EXISTS transaction_partitions (
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partition_index INTEGER NOT NULL,
    PRIMARY KEY (transaction_id, topic_id, partition_index)
);

CREATE TABLE IF NOT EXISTS transaction_groups (
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    group_id TEXT NOT NULL,
    PRIMARY KEY (transaction_id, group_id)
);

CREATE TABLE IF NOT EXISTS transaction_offset_commits (
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    group_id TEXT NOT NULL,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partition_index INTEGER NOT NULL,
    committed_offset BIGINT NOT NULL,
    metadata TEXT,
    PRIMARY KEY (transaction_id, group_id, topic_id, partition_index)
);

ALTER TABLE object_spans
    ADD COLUMN IF NOT EXISTS producer_id BIGINT,
    ADD COLUMN IF NOT EXISTS producer_epoch SMALLINT,
    ADD COLUMN IF NOT EXISTS first_sequence INTEGER,
    ADD COLUMN IF NOT EXISTS last_sequence INTEGER,
    ADD COLUMN IF NOT EXISTS transaction_id UUID REFERENCES transactions(id);

CREATE UNIQUE INDEX IF NOT EXISTS object_spans_idempotence_idx
    ON object_spans (
        topic_id,
        partition_index,
        producer_id,
        producer_epoch,
        first_sequence
    )
    WHERE producer_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS object_spans_pending_txn_idx
    ON object_spans (topic_id, partition_index, base_offset)
    WHERE txn_state = 'pending';

CREATE TABLE IF NOT EXISTS producer_sequences (
    producer_id BIGINT NOT NULL REFERENCES producers(producer_id) ON DELETE CASCADE,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partition_index INTEGER NOT NULL,
    producer_epoch SMALLINT NOT NULL,
    last_sequence INTEGER NOT NULL,
    last_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (producer_id, topic_id, partition_index)
);
