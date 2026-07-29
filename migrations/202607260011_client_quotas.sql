CREATE TABLE client_quotas (
    entity_key TEXT NOT NULL,
    has_user BOOLEAN NOT NULL,
    user_name TEXT,
    has_client_id BOOLEAN NOT NULL,
    client_id TEXT,
    has_ip BOOLEAN NOT NULL,
    ip TEXT,
    quota_key TEXT NOT NULL,
    quota_value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (entity_key, quota_key),
    CONSTRAINT client_quotas_entity_not_empty
        CHECK (has_user OR has_client_id OR has_ip),
    CONSTRAINT client_quotas_absent_names_are_null
        CHECK (
            (has_user OR user_name IS NULL)
            AND (has_client_id OR client_id IS NULL)
            AND (has_ip OR ip IS NULL)
        ),
    CONSTRAINT client_quotas_ip_not_combined
        CHECK (NOT (has_ip AND (has_user OR has_client_id))),
    CONSTRAINT client_quotas_value_positive_finite
        CHECK (
            quota_value > 0
            AND quota_value NOT IN (
                'NaN'::DOUBLE PRECISION,
                'Infinity'::DOUBLE PRECISION,
                '-Infinity'::DOUBLE PRECISION
            )
        )
);
