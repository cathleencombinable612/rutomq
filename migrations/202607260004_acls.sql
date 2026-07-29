ALTER TABLE acl_rules
    ADD COLUMN IF NOT EXISTS host TEXT NOT NULL DEFAULT '*';

CREATE UNIQUE INDEX IF NOT EXISTS acl_rules_unique_idx
    ON acl_rules (
        principal,
        host,
        resource_type,
        resource_name,
        operation,
        permission,
        pattern_type
    );

CREATE INDEX IF NOT EXISTS acl_rules_resource_idx
    ON acl_rules (resource_type, resource_name, pattern_type);
