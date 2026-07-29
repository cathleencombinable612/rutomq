use rutomq_control::{
    FeatureLevelUpdate, FeatureUpgradeType, GROUP_VERSION_FEATURE, MetadataStore,
    PostgresMetadataStore, SHARE_VERSION_FEATURE, STREAMS_VERSION_FEATURE,
    TRANSACTION_VERSION_FEATURE,
};

fn group_update(level: i16, upgrade_type: FeatureUpgradeType) -> FeatureLevelUpdate {
    FeatureLevelUpdate {
        name: GROUP_VERSION_FEATURE.to_owned(),
        max_version_level: level,
        upgrade_type,
    }
}

#[tokio::test]
async fn postgres_feature_updates_are_atomic_and_persistent() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let initial = store.features().await.unwrap();
    assert_eq!(initial.level(SHARE_VERSION_FEATURE), 1);
    assert_eq!(initial.level(TRANSACTION_VERSION_FEATURE), 2);
    assert_eq!(initial.level(STREAMS_VERSION_FEATURE), 1);
    let initial_level = initial.level(GROUP_VERSION_FEATURE);
    let target_level = i16::from(initial_level == 0);
    let target_type = if target_level > initial_level {
        FeatureUpgradeType::Upgrade
    } else {
        FeatureUpgradeType::SafeDowngrade
    };

    let error = store
        .update_features(
            vec![
                group_update(target_level, target_type),
                FeatureLevelUpdate {
                    name: "unknown.feature".to_owned(),
                    max_version_level: 1,
                    upgrade_type: FeatureUpgradeType::Upgrade,
                },
            ],
            false,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unknown.feature"));
    assert_eq!(store.features().await.unwrap(), initial);

    let validated = store
        .update_features(vec![group_update(target_level, target_type)], true)
        .await
        .unwrap();
    assert_eq!(validated, initial);

    let updated = store
        .update_features(vec![group_update(target_level, target_type)], false)
        .await
        .unwrap();
    assert_eq!(updated.epoch, initial.epoch + 1);
    assert_eq!(updated.level(GROUP_VERSION_FEATURE), target_level);

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    assert_eq!(reconnected.features().await.unwrap(), updated);

    let restore_type = if initial_level > target_level {
        FeatureUpgradeType::Upgrade
    } else {
        FeatureUpgradeType::SafeDowngrade
    };
    reconnected
        .update_features(vec![group_update(initial_level, restore_type)], false)
        .await
        .unwrap();
}
