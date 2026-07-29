ALTER TABLE producer_sequences
    ADD COLUMN IF NOT EXISTS last_timestamp BIGINT NOT NULL DEFAULT -1;
