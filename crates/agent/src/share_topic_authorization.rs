use super::Broker;
use super::authorization::AuthorizationContext;
use anyhow::Result;
use rutomq_control::{AclOperation, AclResourceType, TopicInfo};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

pub(super) enum ShareTopicAccess {
    Allowed(TopicInfo),
    Denied,
    Missing,
    MetadataError(String),
}

pub(super) type ShareTopicAccesses = HashMap<Uuid, ShareTopicAccess>;

impl Broker {
    pub(super) async fn share_topic_accesses(
        &self,
        context: &AuthorizationContext,
        topic_ids: impl IntoIterator<Item = Uuid>,
    ) -> Result<ShareTopicAccesses> {
        let mut accesses = HashMap::new();
        for topic_id in topic_ids.into_iter().collect::<BTreeSet<_>>() {
            let access = match self.metadata.topic_by_id(topic_id).await {
                Ok(Some(topic)) => {
                    if self
                        .authorized(
                            context,
                            AclResourceType::Topic,
                            &topic.name,
                            AclOperation::Read,
                        )
                        .await?
                    {
                        ShareTopicAccess::Allowed(topic)
                    } else {
                        ShareTopicAccess::Denied
                    }
                }
                Ok(None) => ShareTopicAccess::Missing,
                Err(error) => ShareTopicAccess::MetadataError(error.to_string()),
            };
            accesses.insert(topic_id, access);
        }
        Ok(accesses)
    }
}
