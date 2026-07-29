use crate::ControlError;
use std::collections::{BTreeMap, HashSet};

pub const METADATA_VERSION_FEATURE: &str = "metadata.version";
pub const TRANSACTION_VERSION_FEATURE: &str = "transaction.version";
pub const GROUP_VERSION_FEATURE: &str = "group.version";
pub const SHARE_VERSION_FEATURE: &str = "share.version";
pub const STREAMS_VERSION_FEATURE: &str = "streams.version";

pub const KAFKA_4_0_IV0: i16 = 22;
pub const KAFKA_4_0_IV1: i16 = 23;
pub const KAFKA_4_0_IV2: i16 = 24;
pub const KAFKA_4_0_IV3: i16 = 25;
pub const KAFKA_4_2_IV0: i16 = 28;
pub const KAFKA_4_2_IV1: i16 = 29;
pub const KAFKA_4_3_IV0: i16 = 30;
pub const TRANSACTION_VERSION_2: i16 = 2;
pub const CONSUMER_GROUP_VERSION: i16 = 1;
pub const SHARE_GROUP_VERSION: i16 = 1;
pub const STREAMS_GROUP_VERSION: i16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SupportedFeature {
    pub name: &'static str,
    pub min_version: i16,
    pub max_version: i16,
}

pub const SUPPORTED_FEATURES: &[SupportedFeature] = &[
    SupportedFeature {
        name: METADATA_VERSION_FEATURE,
        min_version: KAFKA_4_0_IV0,
        max_version: KAFKA_4_3_IV0,
    },
    SupportedFeature {
        name: TRANSACTION_VERSION_FEATURE,
        min_version: 0,
        max_version: TRANSACTION_VERSION_2,
    },
    SupportedFeature {
        name: GROUP_VERSION_FEATURE,
        min_version: 0,
        max_version: CONSUMER_GROUP_VERSION,
    },
    SupportedFeature {
        name: SHARE_VERSION_FEATURE,
        min_version: 0,
        max_version: SHARE_GROUP_VERSION,
    },
    SupportedFeature {
        name: STREAMS_VERSION_FEATURE,
        min_version: 0,
        max_version: STREAMS_GROUP_VERSION,
    },
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureMetadata {
    pub epoch: i64,
    pub finalized: BTreeMap<String, i16>,
}

impl FeatureMetadata {
    pub fn level(&self, name: &str) -> i16 {
        self.finalized.get(name).copied().unwrap_or(0)
    }
}

impl Default for FeatureMetadata {
    fn default() -> Self {
        Self {
            epoch: 0,
            finalized: BTreeMap::from([
                (METADATA_VERSION_FEATURE.to_owned(), KAFKA_4_2_IV1),
                (
                    TRANSACTION_VERSION_FEATURE.to_owned(),
                    TRANSACTION_VERSION_2,
                ),
                (GROUP_VERSION_FEATURE.to_owned(), CONSUMER_GROUP_VERSION),
                (SHARE_VERSION_FEATURE.to_owned(), SHARE_GROUP_VERSION),
                (STREAMS_VERSION_FEATURE.to_owned(), STREAMS_GROUP_VERSION),
            ]),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeatureUpgradeType {
    Upgrade,
    SafeDowngrade,
    UnsafeDowngrade,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureLevelUpdate {
    pub name: String,
    pub max_version_level: i16,
    pub upgrade_type: FeatureUpgradeType,
}

pub(crate) fn apply_updates(
    current: &BTreeMap<String, i16>,
    updates: &[FeatureLevelUpdate],
) -> Result<BTreeMap<String, i16>, ControlError> {
    let mut proposed = current.clone();
    let mut names = HashSet::new();
    for update in updates {
        if update.name.is_empty() {
            return Err(ControlError::InvalidUpdateVersion(
                "feature name must not be empty".to_owned(),
            ));
        }
        if !names.insert(update.name.as_str()) {
            return Err(ControlError::InvalidRequest(format!(
                "feature {} appears more than once",
                update.name
            )));
        }
        if update.max_version_level == 0 {
            proposed.remove(&update.name);
        } else {
            proposed.insert(update.name.clone(), update.max_version_level);
        }
    }

    for update in updates {
        validate_update(current, &proposed, update)?;
    }
    Ok(proposed)
}

fn validate_update(
    current: &BTreeMap<String, i16>,
    proposed: &BTreeMap<String, i16>,
    update: &FeatureLevelUpdate,
) -> Result<(), ControlError> {
    let supported = SUPPORTED_FEATURES
        .iter()
        .find(|feature| feature.name == update.name)
        .ok_or_else(|| invalid_version(update, "the feature is not supported"))?;
    let new_version = update.max_version_level;
    if new_version < supported.min_version || new_version > supported.max_version {
        return Err(invalid_version(
            update,
            &format!(
                "supported versions are {}-{}",
                supported.min_version, supported.max_version
            ),
        ));
    }

    let current_version = current.get(&update.name).copied().unwrap_or(0);
    if new_version < current_version && update.upgrade_type == FeatureUpgradeType::Upgrade {
        return Err(invalid_version(
            update,
            "a downgrade requires a safe or unsafe downgrade type",
        ));
    }
    if new_version > current_version && update.upgrade_type != FeatureUpgradeType::Upgrade {
        return Err(invalid_version(
            update,
            "a downgrade type cannot be used to select a newer version",
        ));
    }

    let crosses_metadata_change = (current_version >= KAFKA_4_0_IV1 && new_version < KAFKA_4_0_IV1)
        || (current_version >= KAFKA_4_3_IV0 && new_version < KAFKA_4_3_IV0);
    if update.name == METADATA_VERSION_FEATURE && crosses_metadata_change {
        let reason = if update.upgrade_type == FeatureUpgradeType::UnsafeDowngrade {
            "unsafe metadata downgrade is not supported"
        } else {
            "the downgrade may delete metadata information"
        };
        return Err(invalid_version(update, reason));
    }

    if [
        TRANSACTION_VERSION_FEATURE,
        GROUP_VERSION_FEATURE,
        SHARE_VERSION_FEATURE,
        STREAMS_VERSION_FEATURE,
    ]
    .iter()
    .any(|feature| proposed.get(*feature).copied().unwrap_or(0) > 0)
        && proposed.get(METADATA_VERSION_FEATURE).copied().unwrap_or(0) < KAFKA_4_0_IV0
    {
        return Err(invalid_version(
            update,
            "nonzero production features require metadata.version>=22",
        ));
    }
    Ok(())
}

fn invalid_version(update: &FeatureLevelUpdate, reason: &str) -> ControlError {
    ControlError::InvalidUpdateVersion(format!(
        "invalid update version {} for feature {}: {reason}",
        update.max_version_level, update.name
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(name: &str, level: i16, upgrade_type: FeatureUpgradeType) -> FeatureLevelUpdate {
        FeatureLevelUpdate {
            name: name.to_owned(),
            max_version_level: level,
            upgrade_type,
        }
    }

    #[test]
    fn validates_atomic_feature_transitions() {
        let current = FeatureMetadata::default().finalized;
        let proposed = apply_updates(
            &current,
            &[update(
                METADATA_VERSION_FEATURE,
                KAFKA_4_2_IV0,
                FeatureUpgradeType::SafeDowngrade,
            )],
        )
        .unwrap();
        assert_eq!(proposed[METADATA_VERSION_FEATURE], KAFKA_4_2_IV0);

        let error = apply_updates(
            &current,
            &[
                update(
                    METADATA_VERSION_FEATURE,
                    KAFKA_4_2_IV0,
                    FeatureUpgradeType::SafeDowngrade,
                ),
                update("unknown.feature", 1, FeatureUpgradeType::Upgrade),
            ],
        )
        .unwrap_err();
        assert!(matches!(error, ControlError::InvalidUpdateVersion(_)));
        assert_eq!(current[METADATA_VERSION_FEATURE], KAFKA_4_2_IV1);
    }

    #[test]
    fn rejects_direction_and_metadata_loss_errors() {
        let current = FeatureMetadata::default().finalized;
        assert!(matches!(
            apply_updates(
                &current,
                &[update(
                    METADATA_VERSION_FEATURE,
                    KAFKA_4_2_IV0,
                    FeatureUpgradeType::Upgrade,
                )]
            ),
            Err(ControlError::InvalidUpdateVersion(_))
        ));
        assert!(matches!(
            apply_updates(
                &current,
                &[update(
                    METADATA_VERSION_FEATURE,
                    22,
                    FeatureUpgradeType::SafeDowngrade,
                )]
            ),
            Err(ControlError::InvalidUpdateVersion(_))
        ));
    }

    #[test]
    fn kafka_43_metadata_upgrade_cannot_be_downgraded() {
        let current = FeatureMetadata::default().finalized;
        assert_eq!(current[METADATA_VERSION_FEATURE], KAFKA_4_2_IV1);
        assert_eq!(
            SUPPORTED_FEATURES
                .iter()
                .find(|feature| feature.name == METADATA_VERSION_FEATURE)
                .unwrap()
                .max_version,
            KAFKA_4_3_IV0
        );

        let upgraded = apply_updates(
            &current,
            &[update(
                METADATA_VERSION_FEATURE,
                KAFKA_4_3_IV0,
                FeatureUpgradeType::Upgrade,
            )],
        )
        .unwrap();
        assert_eq!(upgraded[METADATA_VERSION_FEATURE], KAFKA_4_3_IV0);

        for upgrade_type in [
            FeatureUpgradeType::SafeDowngrade,
            FeatureUpgradeType::UnsafeDowngrade,
        ] {
            let error = apply_updates(
                &upgraded,
                &[update(
                    METADATA_VERSION_FEATURE,
                    KAFKA_4_2_IV1,
                    upgrade_type,
                )],
            )
            .unwrap_err();
            assert!(matches!(error, ControlError::InvalidUpdateVersion(_)));
        }
    }
}
