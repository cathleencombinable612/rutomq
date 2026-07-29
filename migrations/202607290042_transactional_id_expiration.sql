CREATE INDEX IF NOT EXISTS producers_transactional_id_expiration_idx
    ON producers (updated_at, producer_id)
    WHERE transactional_id IS NOT NULL;
