CREATE TABLE IF NOT EXISTS topic_configs (
    topic_id UUID PRIMARY KEY REFERENCES topics(id) ON DELETE CASCADE,
    retention_ms BIGINT NOT NULL DEFAULT 604800000 CHECK (retention_ms >= -1),
    retention_bytes BIGINT NOT NULL DEFAULT -1 CHECK (retention_bytes >= -1),
    cleanup_policy TEXT NOT NULL DEFAULT 'delete'
        CHECK (cleanup_policy IN ('delete', 'compact', 'compact,delete', 'delete,compact')),
    delete_retention_ms BIGINT NOT NULL DEFAULT 86400000 CHECK (delete_retention_ms >= 0),
    min_compaction_lag_ms BIGINT NOT NULL DEFAULT 0 CHECK (min_compaction_lag_ms >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO topic_configs (topic_id)
SELECT id FROM topics
ON CONFLICT (topic_id) DO NOTHING;

ALTER TABLE objects
    ADD COLUMN IF NOT EXISTS unreferenced_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS objects_unreferenced_idx
    ON objects (unreferenced_at)
    WHERE unreferenced_at IS NOT NULL;
