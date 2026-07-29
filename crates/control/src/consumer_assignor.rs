use crate::consumer_groups::{ConsumerGroupState, ConsumerTopicAssignment};
use crate::{ControlError, TopicInfo};
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

pub(crate) const UNIFORM_ASSIGNOR: &str = "uniform";
pub(crate) const RANGE_ASSIGNOR: &str = "range";

pub(crate) fn validate_assignor(name: &str) -> Result<(), ControlError> {
    match name {
        UNIFORM_ASSIGNOR | RANGE_ASSIGNOR => Ok(()),
        _ => Err(ControlError::UnsupportedConsumerAssignor(name.to_owned())),
    }
}

pub(crate) fn assign_with_regex_topics(
    group: &ConsumerGroupState,
    topics: &[TopicInfo],
    resolved_regex_topics: Option<&BTreeSet<Uuid>>,
) -> Result<HashMap<String, Vec<ConsumerTopicAssignment>>, ControlError> {
    validate_assignor(&group.assignor_name)?;
    let subscriptions = subscriptions(group, topics, resolved_regex_topics)?;
    let assigned = match group.assignor_name.as_str() {
        RANGE_ASSIGNOR => range_assign(topics, &subscriptions),
        _ => uniform_assign(topics, &subscriptions),
    };
    Ok(to_assignments(assigned, topics))
}

fn subscriptions(
    group: &ConsumerGroupState,
    topics: &[TopicInfo],
    resolved_regex_topics: Option<&BTreeSet<Uuid>>,
) -> Result<BTreeMap<String, BTreeSet<Uuid>>, ControlError> {
    let mut result = BTreeMap::new();
    for (member_id, member) in &group.members {
        let regex = member
            .subscribed_topic_regex
            .as_deref()
            .map(|pattern| Regex::new(&format!("^(?:{pattern})$")))
            .transpose()
            .map_err(|error| {
                ControlError::InvalidRequest(format!(
                    "invalid subscribed topic regex for {member_id}: {error}"
                ))
            })?;
        let explicit = member
            .subscribed_topic_names
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let subscribed = topics
            .iter()
            .filter(|topic| {
                explicit.contains(topic.name.as_str())
                    || regex.as_ref().is_some_and(|pattern| {
                        resolved_regex_topics.is_none_or(|resolved| resolved.contains(&topic.id))
                            && pattern.is_match(&topic.name)
                    })
            })
            .map(|topic| topic.id)
            .collect();
        result.insert(member_id.clone(), subscribed);
    }
    Ok(result)
}

fn range_assign(
    topics: &[TopicInfo],
    subscriptions: &BTreeMap<String, BTreeSet<Uuid>>,
) -> BTreeMap<String, BTreeMap<Uuid, BTreeSet<i32>>> {
    let mut result = empty_assignments(subscriptions);
    let mut ordered_topics = topics.iter().collect::<Vec<_>>();
    ordered_topics.sort_by(|left, right| left.name.cmp(&right.name));
    for topic in ordered_topics {
        let members = subscribed_members(subscriptions, topic.id);
        if members.is_empty() {
            continue;
        }
        let base = topic.partitions as usize / members.len();
        let extra = topic.partitions as usize % members.len();
        let mut next_partition = 0;
        for (index, member_id) in members.into_iter().enumerate() {
            let count = base + usize::from(index < extra);
            let partitions = result
                .get_mut(member_id)
                .expect("subscribed member has an assignment")
                .entry(topic.id)
                .or_default();
            for _ in 0..count {
                partitions.insert(next_partition);
                next_partition += 1;
            }
        }
    }
    result
}

fn uniform_assign(
    topics: &[TopicInfo],
    subscriptions: &BTreeMap<String, BTreeSet<Uuid>>,
) -> BTreeMap<String, BTreeMap<Uuid, BTreeSet<i32>>> {
    let mut result = empty_assignments(subscriptions);
    let mut loads = subscriptions
        .keys()
        .map(|member_id| (member_id.clone(), 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut ordered_topics = topics.iter().collect::<Vec<_>>();
    ordered_topics.sort_by(|left, right| left.name.cmp(&right.name));

    for topic in ordered_topics {
        let members = subscribed_members(subscriptions, topic.id);
        for partition in 0..topic.partitions {
            let Some(member_id) = members
                .iter()
                .min_by_key(|member_id| {
                    (
                        loads.get(**member_id).copied().unwrap_or_default(),
                        member_id.as_str(),
                    )
                })
                .copied()
            else {
                continue;
            };
            result
                .get_mut(member_id)
                .expect("subscribed member has an assignment")
                .entry(topic.id)
                .or_default()
                .insert(partition);
            *loads
                .get_mut(member_id)
                .expect("subscribed member has a load") += 1;
        }
    }
    result
}

fn subscribed_members(
    subscriptions: &BTreeMap<String, BTreeSet<Uuid>>,
    topic_id: Uuid,
) -> Vec<&String> {
    subscriptions
        .iter()
        .filter_map(|(member_id, topics)| topics.contains(&topic_id).then_some(member_id))
        .collect()
}

fn empty_assignments(
    subscriptions: &BTreeMap<String, BTreeSet<Uuid>>,
) -> BTreeMap<String, BTreeMap<Uuid, BTreeSet<i32>>> {
    subscriptions
        .keys()
        .map(|member_id| (member_id.clone(), BTreeMap::new()))
        .collect()
}

fn to_assignments(
    assigned: BTreeMap<String, BTreeMap<Uuid, BTreeSet<i32>>>,
    topics: &[TopicInfo],
) -> HashMap<String, Vec<ConsumerTopicAssignment>> {
    let topic_names = topics
        .iter()
        .map(|topic| (topic.id, topic.name.as_str()))
        .collect::<HashMap<_, _>>();
    assigned
        .into_iter()
        .map(|(member_id, topics)| {
            let assignment = topics
                .into_iter()
                .filter_map(|(topic_id, partitions)| {
                    let topic_name = topic_names.get(&topic_id)?;
                    Some(ConsumerTopicAssignment {
                        topic_id,
                        topic_name: (*topic_name).to_owned(),
                        partitions: partitions.into_iter().collect(),
                    })
                })
                .collect();
            (member_id, assignment)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer_groups::ConsumerMemberState;
    use chrono::Utc;

    fn member(id: &str, topics: &[&str]) -> (String, ConsumerMemberState) {
        (
            id.to_owned(),
            ConsumerMemberState {
                member_id: id.to_owned(),
                subscribed_topic_names: topics.iter().map(|topic| (*topic).to_owned()).collect(),
                last_heartbeat: Utc::now(),
                ..ConsumerMemberState::default()
            },
        )
    }

    fn topic(name: &str, partitions: i32) -> TopicInfo {
        TopicInfo {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            partitions,
        }
    }

    #[test]
    fn uniform_balances_across_topics() {
        let topics = [topic("a", 3), topic("b", 3)];
        let group = ConsumerGroupState {
            members: HashMap::from([
                member("member-a", &["a", "b"]),
                member("member-b", &["a", "b"]),
            ]),
            ..ConsumerGroupState::default()
        };
        let assignments = assign_with_regex_topics(&group, &topics, None).unwrap();
        let counts = assignments
            .values()
            .map(|assignment| {
                assignment
                    .iter()
                    .map(|topic| topic.partitions.len())
                    .sum::<usize>()
            })
            .collect::<Vec<_>>();
        assert_eq!(counts, [3, 3]);
    }

    #[test]
    fn range_keeps_per_topic_partitions_contiguous() {
        let topic = topic("orders", 5);
        let group = ConsumerGroupState {
            assignor_name: RANGE_ASSIGNOR.to_owned(),
            members: HashMap::from([
                member("member-a", &["orders"]),
                member("member-b", &["orders"]),
            ]),
            ..ConsumerGroupState::default()
        };
        let assignments =
            assign_with_regex_topics(&group, std::slice::from_ref(&topic), None).unwrap();
        assert_eq!(assignments["member-a"][0].partitions, [0, 1, 2]);
        assert_eq!(assignments["member-b"][0].partitions, [3, 4]);
    }

    #[test]
    fn regex_subscription_uses_full_topic_name() {
        let topics = [topic("orders-us", 1), topic("not-orders-us-copy", 1)];
        let mut subscribed = member("member-a", &[]).1;
        subscribed.subscribed_topic_regex = Some("orders-.*".to_owned());
        let group = ConsumerGroupState {
            members: HashMap::from([("member-a".to_owned(), subscribed)]),
            ..ConsumerGroupState::default()
        };
        let assignments = assign_with_regex_topics(&group, &topics, None).unwrap();
        assert_eq!(assignments["member-a"].len(), 1);
        assert_eq!(assignments["member-a"][0].topic_name, "orders-us");
    }
}
