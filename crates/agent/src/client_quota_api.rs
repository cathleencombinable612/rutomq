use super::Broker;
use super::authorization::{AuthorizationContext, CLUSTER_RESOURCE_NAME};
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, INVALID_REQUEST, NO_ERROR, UNKNOWN_SERVER_ERROR,
    UNSUPPORTED_VERSION,
};
use kafka_protocol::messages::alter_client_quotas_request::EntryData as AlterRequestEntry;
use kafka_protocol::messages::alter_client_quotas_response::{
    EntityData as AlterResponseEntity, EntryData as AlterResponseEntry,
};
use kafka_protocol::messages::describe_client_quotas_request::ComponentData;
use kafka_protocol::messages::describe_client_quotas_response::{
    EntityData as DescribeResponseEntity, EntryData as DescribeResponseEntry, ValueData,
};
use kafka_protocol::messages::{
    AlterClientQuotasRequest, AlterClientQuotasResponse, DescribeClientQuotasRequest,
    DescribeClientQuotasResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    AclOperation, AclResourceType, CLIENT_ID_ENTITY, CONNECTION_CREATION_RATE, CONSUMER_BYTE_RATE,
    CONTROLLER_MUTATION_RATE, ClientQuota, ClientQuotaAlteration, ClientQuotaEntity, IP_ENTITY,
    PRODUCER_BYTE_RATE, REQUEST_PERCENTAGE, USER_ENTITY,
};
use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::str::FromStr;

impl Broker {
    pub(super) async fn handle_describe_client_quotas(
        &self,
        request: DescribeClientQuotasRequest,
        context: &AuthorizationContext,
    ) -> DescribeClientQuotasResponse {
        if !self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::DescribeConfigs,
            )
            .await
            .unwrap_or(false)
        {
            return describe_error(CLUSTER_AUTHORIZATION_FAILED, "cluster authorization failed");
        }
        let filters = match parse_filters(request.components) {
            Ok(filters) => filters,
            Err((code, error)) => return describe_error(code, &error),
        };
        match self.metadata.client_quotas().await {
            Ok(quotas) => DescribeClientQuotasResponse::default()
                .with_error_code(NO_ERROR)
                .with_error_message(None)
                .with_entries(Some(
                    quotas
                        .into_iter()
                        .filter(|quota| filters.matches(&quota.entity, request.strict))
                        .map(describe_entry)
                        .collect(),
                )),
            Err(error) => describe_error(UNKNOWN_SERVER_ERROR, &error.to_string()),
        }
    }

    pub(super) async fn handle_alter_client_quotas(
        &self,
        request: AlterClientQuotasRequest,
        context: &AuthorizationContext,
    ) -> AlterClientQuotasResponse {
        let authorized = self
            .authorized(
                context,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
                AclOperation::AlterConfigs,
            )
            .await
            .unwrap_or(false);
        if !authorized {
            return alter_response(
                request
                    .entries
                    .into_iter()
                    .map(|entry| {
                        alter_result(
                            request_entities(&entry),
                            CLUSTER_AUTHORIZATION_FAILED,
                            Some("cluster authorization failed"),
                        )
                    })
                    .collect(),
            );
        }

        let duplicate_entities = duplicate_entities(&request.entries);
        let mut results = Vec::with_capacity(request.entries.len());
        let mut alterations = Vec::new();
        let mut valid_result_indexes = Vec::new();
        for entry in request.entries {
            let response_entity = request_entities(&entry);
            match parse_alteration(entry) {
                Ok(alteration) if duplicate_entities.contains_key(&alteration.entity) => {
                    results.push(alter_result(
                        response_entity,
                        INVALID_REQUEST,
                        Some("the same quota entity cannot be altered twice"),
                    ));
                }
                Ok(alteration) => {
                    valid_result_indexes.push(results.len());
                    alterations.push(alteration);
                    results.push(alter_result(response_entity, NO_ERROR, None));
                }
                Err(error) => {
                    results.push(alter_result(response_entity, INVALID_REQUEST, Some(&error)))
                }
            }
        }

        if !request.validate_only && !alterations.is_empty() {
            match self.metadata.alter_client_quotas(alterations).await {
                Ok(()) => self.quotas.invalidate().await,
                Err(error) => {
                    for index in valid_result_indexes {
                        results[index].error_code = UNKNOWN_SERVER_ERROR;
                        results[index].error_message =
                            Some(StrBytes::from_string(error.to_string()));
                    }
                }
            }
        }
        alter_response(results)
    }
}

fn parse_alteration(entry: AlterRequestEntry) -> Result<ClientQuotaAlteration, String> {
    let entity = parse_entity(
        entry
            .entity
            .iter()
            .map(|part| {
                (
                    part.entity_type.as_str(),
                    part.entity_name.as_ref().map(|name| name.as_str()),
                )
            })
            .collect(),
    )?;
    let mut ops = BTreeMap::new();
    for op in entry.ops {
        let key = op.key.as_str();
        if ops.contains_key(key) {
            return Err(format!("quota key {key} is duplicated"));
        }
        if !op.remove {
            validate_value(&entity, key, op.value)?;
        } else {
            validate_key(&entity, key)?;
        }
        ops.insert(key.to_owned(), (!op.remove).then_some(op.value));
    }
    Ok(ClientQuotaAlteration { entity, ops })
}

fn parse_entity(parts: Vec<(&str, Option<&str>)>) -> Result<ClientQuotaEntity, String> {
    if parts.is_empty() {
        return Err("quota entity must not be empty".to_owned());
    }
    let mut entity = ClientQuotaEntity::default();
    for (entity_type, name) in parts {
        if name.is_some_and(str::is_empty) {
            return Err(format!("{entity_type} entity name must not be empty"));
        }
        let target = match entity_type {
            USER_ENTITY => &mut entity.user,
            CLIENT_ID_ENTITY => &mut entity.client_id,
            IP_ENTITY => &mut entity.ip,
            "" => return Err("quota entity type must not be empty".to_owned()),
            _ => return Err(format!("unsupported quota entity type {entity_type}")),
        };
        if target.is_some() {
            return Err(format!("quota entity type {entity_type} is duplicated"));
        }
        *target = Some(name.map(str::to_owned));
    }
    if entity.ip.is_some() && (entity.user.is_some() || entity.client_id.is_some()) {
        return Err("IP quota entities cannot be combined with user or client-id".to_owned());
    }
    if let Some(Some(ip)) = &entity.ip {
        IpAddr::from_str(ip).map_err(|_| format!("invalid IP quota entity {ip}"))?;
    }
    Ok(entity)
}

fn validate_key(entity: &ClientQuotaEntity, key: &str) -> Result<(), String> {
    let valid = if entity.ip.is_some() {
        key == CONNECTION_CREATION_RATE
    } else {
        matches!(
            key,
            PRODUCER_BYTE_RATE | CONSUMER_BYTE_RATE | REQUEST_PERCENTAGE | CONTROLLER_MUTATION_RATE
        )
    };
    valid
        .then_some(())
        .ok_or_else(|| format!("quota key {key} is not valid for this entity"))
}

fn validate_value(entity: &ClientQuotaEntity, key: &str, value: f64) -> Result<(), String> {
    validate_key(entity, key)?;
    if !value.is_finite() || value <= 0.0 {
        return Err(format!("quota value for {key} must be positive and finite"));
    }
    let maximum = match key {
        PRODUCER_BYTE_RATE | CONSUMER_BYTE_RATE => Some(i64::MAX as f64),
        CONNECTION_CREATION_RATE => Some(i32::MAX as f64),
        _ => None,
    };
    if let Some(maximum) = maximum
        && ((value - value.round()).abs() > 0.000_001 || value > maximum)
    {
        return Err(format!("quota value for {key} must be an in-range integer"));
    }
    Ok(())
}

#[derive(Default)]
struct QuotaFilters {
    components: BTreeMap<&'static str, FilterMatch>,
}

enum FilterMatch {
    Exact(String),
    Default,
    Specified,
}

impl QuotaFilters {
    fn matches(&self, entity: &ClientQuotaEntity, strict: bool) -> bool {
        if strict && entity.dimension_count() != self.components.len() {
            return false;
        }
        self.components.iter().all(|(entity_type, filter)| {
            let value = match *entity_type {
                USER_ENTITY => &entity.user,
                CLIENT_ID_ENTITY => &entity.client_id,
                IP_ENTITY => &entity.ip,
                _ => return false,
            };
            match filter {
                FilterMatch::Exact(expected) => value
                    .as_ref()
                    .is_some_and(|value| value.as_ref() == Some(expected)),
                FilterMatch::Default => value == &Some(None),
                FilterMatch::Specified => value.is_some(),
            }
        })
    }
}

fn parse_filters(components: Vec<ComponentData>) -> Result<QuotaFilters, (i16, String)> {
    let mut filters = QuotaFilters::default();
    for component in components {
        let entity_type = match component.entity_type.as_str() {
            USER_ENTITY => USER_ENTITY,
            CLIENT_ID_ENTITY => CLIENT_ID_ENTITY,
            IP_ENTITY => IP_ENTITY,
            "" => {
                return Err((
                    INVALID_REQUEST,
                    "quota filter entity type must not be empty".to_owned(),
                ));
            }
            other => {
                return Err((
                    UNSUPPORTED_VERSION,
                    format!("unsupported quota filter entity type {other}"),
                ));
            }
        };
        if filters.components.contains_key(entity_type) {
            return Err((
                INVALID_REQUEST,
                format!("quota filter entity type {entity_type} is duplicated"),
            ));
        }
        let value = match (component.match_type, component._match) {
            (0, Some(value)) => FilterMatch::Exact(value.as_str().to_owned()),
            (1, None) => FilterMatch::Default,
            (2, None) => FilterMatch::Specified,
            (0, None) => {
                return Err((
                    INVALID_REQUEST,
                    "exact quota filter requires a match value".to_owned(),
                ));
            }
            (1 | 2, Some(_)) => {
                return Err((
                    INVALID_REQUEST,
                    "default and specified quota filters must not include a match value".to_owned(),
                ));
            }
            (match_type, _) => {
                return Err((
                    INVALID_REQUEST,
                    format!("unsupported quota filter match type {match_type}"),
                ));
            }
        };
        filters.components.insert(entity_type, value);
    }
    if filters.components.contains_key(IP_ENTITY)
        && (filters.components.contains_key(USER_ENTITY)
            || filters.components.contains_key(CLIENT_ID_ENTITY))
    {
        return Err((
            INVALID_REQUEST,
            "IP quota filters cannot be combined with user or client-id".to_owned(),
        ));
    }
    Ok(filters)
}

fn duplicate_entities(entries: &[AlterRequestEntry]) -> HashMap<ClientQuotaEntity, usize> {
    let mut counts = HashMap::new();
    for entry in entries {
        if let Ok(entity) = parse_entity(
            entry
                .entity
                .iter()
                .map(|part| {
                    (
                        part.entity_type.as_str(),
                        part.entity_name.as_ref().map(|name| name.as_str()),
                    )
                })
                .collect(),
        ) {
            *counts.entry(entity).or_insert(0) += 1;
        }
    }
    counts.retain(|_, count| *count > 1);
    counts
}

fn request_entities(entry: &AlterRequestEntry) -> Vec<AlterResponseEntity> {
    entry
        .entity
        .iter()
        .map(|part| {
            AlterResponseEntity::default()
                .with_entity_type(part.entity_type.clone())
                .with_entity_name(part.entity_name.clone())
        })
        .collect()
}

fn describe_entry(quota: ClientQuota) -> DescribeResponseEntry {
    DescribeResponseEntry::default()
        .with_entity(
            quota
                .entity
                .dimensions()
                .into_iter()
                .map(|(entity_type, name)| {
                    DescribeResponseEntity::default()
                        .with_entity_type(StrBytes::from_static_str(entity_type))
                        .with_entity_name(name.map(|name| StrBytes::from_string(name.to_owned())))
                })
                .collect(),
        )
        .with_values(
            quota
                .values
                .into_iter()
                .map(|(key, value)| {
                    ValueData::default()
                        .with_key(StrBytes::from_string(key))
                        .with_value(value)
                })
                .collect(),
        )
}

fn describe_error(error_code: i16, error: &str) -> DescribeClientQuotasResponse {
    DescribeClientQuotasResponse::default()
        .with_error_code(error_code)
        .with_error_message(Some(StrBytes::from_string(error.to_owned())))
        .with_entries(None)
}

fn alter_result(
    entity: Vec<AlterResponseEntity>,
    error_code: i16,
    error: Option<&str>,
) -> AlterResponseEntry {
    AlterResponseEntry::default()
        .with_error_code(error_code)
        .with_error_message(error.map(|error| StrBytes::from_string(error.to_owned())))
        .with_entity(entity)
}

fn alter_response(entries: Vec<AlterResponseEntry>) -> AlterClientQuotasResponse {
    AlterClientQuotasResponse::default().with_entries(entries)
}
