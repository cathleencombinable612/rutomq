ALTER TABLE objects
    ADD COLUMN committed BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX objects_pending_created_idx
    ON objects (created_at, object_key)
    WHERE committed = FALSE;
