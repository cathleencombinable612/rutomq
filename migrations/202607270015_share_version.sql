INSERT INTO cluster_features (feature_name, version_level)
VALUES ('share.version', 1)
ON CONFLICT (feature_name) DO NOTHING;
