CREATE TABLE client_metric_configs (
    subscription_name TEXT NOT NULL,
    config_key TEXT NOT NULL,
    config_value TEXT NOT NULL,
    PRIMARY KEY (subscription_name, config_key),
    CONSTRAINT client_metric_configs_name_not_empty
        CHECK (subscription_name <> ''),
    CONSTRAINT client_metric_configs_known_key
        CHECK (config_key IN ('metrics', 'interval.ms', 'match'))
);
