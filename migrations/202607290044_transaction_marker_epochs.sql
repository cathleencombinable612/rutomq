ALTER TABLE transactions
    ADD COLUMN marker_producer_epoch SMALLINT,
    ADD COLUMN marker_coordinator_epoch INTEGER,
    ADD COLUMN marker_transaction_version SMALLINT;
