use crate::{
    AclFilter, AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule, ControlError,
    acls::{authorize_by_resource_type_rules, authorize_rules},
};
use sqlx::{PgPool, Postgres, Row, Transaction};

pub(crate) async fn create(pool: &PgPool, rule: &AclRule) -> Result<(), ControlError> {
    rule.validate()?;
    sqlx::query(
        "INSERT INTO acl_rules
         (principal, host, resource_type, resource_name, operation, permission, pattern_type)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT DO NOTHING",
    )
    .bind(&rule.principal)
    .bind(&rule.host)
    .bind(rule.resource_type.name())
    .bind(&rule.resource_name)
    .bind(rule.operation.name())
    .bind(rule.permission.name())
    .bind(rule.pattern_type.name())
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn describe(
    pool: &PgPool,
    filter: &AclFilter,
) -> Result<Vec<AclRule>, ControlError> {
    let rows = sqlx::query(
        "SELECT resource_type, resource_name, pattern_type, principal, host, operation, permission
         FROM acl_rules
         ORDER BY resource_type, resource_name, pattern_type, principal, host, operation, permission",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(row_to_rule)
        .filter_map(|rule| match rule {
            Ok(rule) if filter.matches(&rule) => Some(Ok(rule)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

pub(crate) async fn delete(
    pool: &PgPool,
    filters: &[AclFilter],
) -> Result<Vec<Vec<AclRule>>, ControlError> {
    let mut transaction = pool.begin().await?;
    let mut results = Vec::with_capacity(filters.len());
    for filter in filters {
        results.push(delete_filter(&mut transaction, filter).await?);
    }
    transaction.commit().await?;
    Ok(results)
}

async fn delete_filter(
    transaction: &mut Transaction<'_, Postgres>,
    filter: &AclFilter,
) -> Result<Vec<AclRule>, ControlError> {
    let rows = sqlx::query(
        "SELECT id, resource_type, resource_name, pattern_type, principal, host, operation, permission
         FROM acl_rules
         ORDER BY id
         FOR UPDATE",
    )
    .fetch_all(&mut **transaction)
    .await?;
    let mut deleted = Vec::new();
    for row in rows {
        let id: i64 = row.get("id");
        let rule = row_to_rule(row)?;
        if filter.matches(&rule) {
            sqlx::query("DELETE FROM acl_rules WHERE id = $1")
                .bind(id)
                .execute(&mut **transaction)
                .await?;
            deleted.push(rule);
        }
    }
    Ok(deleted)
}

pub(crate) async fn authorize(
    pool: &PgPool,
    principal: &str,
    host: &str,
    resource_type: AclResourceType,
    resource_name: &str,
    operation: AclOperation,
    allow_if_no_acl: bool,
) -> Result<bool, ControlError> {
    let rows = sqlx::query(
        "SELECT resource_type, resource_name, pattern_type, principal, host, operation, permission
         FROM acl_rules
         WHERE resource_type = $1",
    )
    .bind(resource_type.name())
    .fetch_all(pool)
    .await?;
    let rules = rows
        .into_iter()
        .map(row_to_rule)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(authorize_rules(
        &rules,
        principal,
        host,
        resource_type,
        resource_name,
        operation,
        allow_if_no_acl,
    ))
}

pub(crate) async fn authorize_by_resource_type(
    pool: &PgPool,
    principal: &str,
    host: &str,
    resource_type: AclResourceType,
    operation: AclOperation,
    allow_if_no_acl: bool,
) -> Result<bool, ControlError> {
    let rows = sqlx::query(
        "SELECT resource_type, resource_name, pattern_type, principal, host, operation, permission
         FROM acl_rules
         WHERE resource_type = $1",
    )
    .bind(resource_type.name())
    .fetch_all(pool)
    .await?;
    let rules = rows
        .into_iter()
        .map(row_to_rule)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(authorize_by_resource_type_rules(
        &rules,
        principal,
        host,
        resource_type,
        operation,
        allow_if_no_acl,
    ))
}

fn row_to_rule(row: sqlx::postgres::PgRow) -> Result<AclRule, ControlError> {
    Ok(AclRule {
        resource_type: AclResourceType::from_name(row.get("resource_type"))?,
        resource_name: row.get("resource_name"),
        pattern_type: AclPatternType::from_name(row.get("pattern_type"))?,
        principal: row.get("principal"),
        host: row.get("host"),
        operation: AclOperation::from_name(row.get("operation"))?,
        permission: AclPermission::from_name(row.get("permission"))?,
    })
}
