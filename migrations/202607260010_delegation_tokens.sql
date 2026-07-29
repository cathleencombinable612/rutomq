CREATE TABLE delegation_tokens (
    token_id TEXT PRIMARY KEY,
    owner_principal TEXT NOT NULL,
    requester_principal TEXT NOT NULL,
    renewers TEXT[] NOT NULL DEFAULT '{}',
    issue_timestamp_ms BIGINT NOT NULL,
    expiry_timestamp_ms BIGINT NOT NULL,
    max_timestamp_ms BIGINT NOT NULL,
    hmac BYTEA NOT NULL UNIQUE,
    CONSTRAINT delegation_tokens_owner_not_empty
        CHECK (owner_principal <> ''),
    CONSTRAINT delegation_tokens_requester_not_empty
        CHECK (requester_principal <> ''),
    CONSTRAINT delegation_tokens_hmac_not_empty
        CHECK (octet_length(hmac) > 0),
    CONSTRAINT delegation_tokens_timestamps_valid
        CHECK (
            issue_timestamp_ms <= expiry_timestamp_ms
            AND expiry_timestamp_ms <= max_timestamp_ms
        )
);

CREATE INDEX delegation_tokens_expiry_idx
    ON delegation_tokens (expiry_timestamp_ms);
