use rutomq_control::{
    FeatureLevelUpdate, FeatureUpgradeType, GROUP_VERSION_FEATURE, MemoryMetadataStore,
    MetadataStore, STREAMS_VERSION_FEATURE, TRANSACTION_VERSION_FEATURE,
};

fn group_update(level: i16, upgrade_type: FeatureUpgradeType) -> FeatureLevelUpdate {
    FeatureLevelUpdate {
        name: GROUP_VERSION_FEATURE.to_owned(),
        max_version_level: level,
        upgrade_type,
    }
}

#[tokio::test]
async fn memory_feature_updates_are_atomic_and_versioned() {
    let store = MemoryMetadataStore::new();
    let initial = store.features().await.unwrap();
    assert_eq!(initial.level(GROUP_VERSION_FEATURE), 1);
    assert_eq!(initial.level(TRANSACTION_VERSION_FEATURE), 2);
    assert_eq!(initial.level(STREAMS_VERSION_FEATURE), 1);

    let validated = store
        .update_features(
            vec![group_update(0, FeatureUpgradeType::SafeDowngrade)],
            true,
        )
        .await
        .unwrap();
    assert_eq!(validated, initial);

    let error = store
        .update_features(
            vec![
                group_update(0, FeatureUpgradeType::SafeDowngrade),
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

    let updated = store
        .update_features(
            vec![group_update(0, FeatureUpgradeType::SafeDowngrade)],
            false,
        )
        .await
        .unwrap();
    assert_eq!(updated.epoch, initial.epoch + 1);
    assert_eq!(updated.level(GROUP_VERSION_FEATURE), 0);

    let unchanged = store
        .update_features(
            vec![group_update(0, FeatureUpgradeType::SafeDowngrade)],
            false,
        )
        .await
        .unwrap();
    assert_eq!(unchanged.epoch, updated.epoch);
}
