use rutomq_control::{
    ClientQuotaAlteration, ClientQuotaEntity, MemoryMetadataStore, MetadataStore,
};
use std::collections::BTreeMap;

fn alteration(entity: ClientQuotaEntity, ops: &[(&str, Option<f64>)]) -> ClientQuotaAlteration {
    ClientQuotaAlteration {
        entity,
        ops: ops
            .iter()
            .map(|(key, value)| ((*key).to_owned(), *value))
            .collect::<BTreeMap<_, _>>(),
    }
}

#[tokio::test]
async fn memory_client_quotas_set_replace_and_remove_values() {
    let store = MemoryMetadataStore::new();
    let entity = ClientQuotaEntity {
        user: Some(Some("alice".to_owned())),
        client_id: Some(None),
        ip: None,
    };
    store
        .alter_client_quotas(vec![alteration(
            entity.clone(),
            &[
                ("producer_byte_rate", Some(1_024.0)),
                ("request_percentage", Some(20.0)),
            ],
        )])
        .await
        .unwrap();
    store
        .alter_client_quotas(vec![alteration(
            entity.clone(),
            &[
                ("producer_byte_rate", Some(2_048.0)),
                ("request_percentage", None),
            ],
        )])
        .await
        .unwrap();

    let quotas = store.client_quotas().await.unwrap();
    assert_eq!(quotas.len(), 1);
    assert_eq!(quotas[0].entity, entity);
    assert_eq!(quotas[0].values["producer_byte_rate"], 2_048.0);
    assert!(!quotas[0].values.contains_key("request_percentage"));

    store
        .alter_client_quotas(vec![alteration(
            quotas[0].entity.clone(),
            &[("producer_byte_rate", None)],
        )])
        .await
        .unwrap();
    assert!(store.client_quotas().await.unwrap().is_empty());
}

#[test]
fn client_quota_storage_keys_do_not_alias_defaults_or_names() {
    let named = ClientQuotaEntity {
        user: Some(Some("d".to_owned())),
        client_id: None,
        ip: None,
    };
    let default = ClientQuotaEntity {
        user: Some(None),
        client_id: None,
        ip: None,
    };
    assert_ne!(named, default);
    assert_eq!(named.dimension_count(), 1);
    assert_eq!(default.dimensions(), vec![("user", None)]);
}
