ALTER TABLE object_spans
    ADD COLUMN IF NOT EXISTS format_version SMALLINT NOT NULL DEFAULT 0
        CHECK (format_version >= 0),
    ADD COLUMN IF NOT EXISTS checksum BYTEA;

ALTER TABLE object_spans
    DROP CONSTRAINT IF EXISTS object_spans_integrity_check;

ALTER TABLE object_spans
    ADD CONSTRAINT object_spans_integrity_check CHECK (
        (format_version = 0 AND checksum IS NULL)
        OR
        (format_version > 0 AND checksum IS NOT NULL AND octet_length(checksum) = 32)
    );
