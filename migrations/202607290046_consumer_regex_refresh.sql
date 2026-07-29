ALTER TABLE consumer_protocol_groups
    ADD COLUMN IF NOT EXISTS regex_refresh_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS regex_refresh_pending BOOLEAN NOT NULL DEFAULT FALSE;
