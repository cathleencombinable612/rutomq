ALTER TABLE objects
    ADD COLUMN IF NOT EXISTS orphan_gc_claimed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS objects_orphan_gc_claim_idx
    ON objects (orphan_gc_claimed_at, created_at, object_key)
    WHERE committed = FALSE;
