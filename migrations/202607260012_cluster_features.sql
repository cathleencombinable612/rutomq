CREATE TABLE IF NOT EXISTS cluster_feature_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    epoch BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0)
);

INSERT INTO cluster_feature_state (singleton, epoch)
VALUES (TRUE, 0)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS cluster_features (
    feature_name TEXT PRIMARY KEY CHECK (feature_name <> ''),
    version_level SMALLINT NOT NULL CHECK (version_level > 0)
);

INSERT INTO cluster_features (feature_name, version_level)
VALUES
    ('metadata.version', 25),
    ('group.version', 1)
ON CONFLICT (feature_name) DO NOTHING;
