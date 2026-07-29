CREATE TABLE IF NOT EXISTS scram_credentials (
    username TEXT NOT NULL,
    mechanism SMALLINT NOT NULL CHECK (mechanism IN (1, 2)),
    iterations INTEGER NOT NULL CHECK (iterations BETWEEN 4096 AND 16384),
    salt BYTEA NOT NULL,
    stored_key BYTEA NOT NULL,
    server_key BYTEA NOT NULL,
    PRIMARY KEY (username, mechanism)
);
