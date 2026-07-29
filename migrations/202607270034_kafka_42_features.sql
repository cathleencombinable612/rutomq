UPDATE cluster_features
SET version_level = 29
WHERE feature_name = 'metadata.version'
  AND version_level = 25;

INSERT INTO cluster_features (feature_name, version_level)
VALUES
    ('transaction.version', 2),
    ('streams.version', 1)
ON CONFLICT (feature_name) DO NOTHING;
