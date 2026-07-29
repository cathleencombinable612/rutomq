use kafka_protocol::messages::streams_group_heartbeat_request::Topology;
use rutomq_control::validate_topic_name;
use std::collections::HashSet;

const INTERNAL_TOPICS: [&str; 3] = [
    "__consumer_offsets",
    "__transaction_state",
    "__share_group_state",
];

pub(super) fn validate(topology: &Topology) -> Result<(), String> {
    let names = topic_names(topology);
    let internal = names
        .iter()
        .copied()
        .filter(|name| INTERNAL_TOPICS.contains(name))
        .collect::<Vec<_>>();
    if !internal.is_empty() {
        return Err(format!(
            "Use of Kafka internal topics {} in a Kafka Streams topology is prohibited.",
            internal.join(",")
        ));
    }

    let invalid = names
        .iter()
        .copied()
        .filter(|name| validate_topic_name(name).is_err())
        .collect::<Vec<_>>();
    if !invalid.is_empty() {
        return Err(format!(
            "Topic names {} are not valid topic names.",
            invalid.join(",")
        ));
    }
    Ok(())
}

fn topic_names(topology: &Topology) -> Vec<&str> {
    let mut seen = HashSet::new();
    let mut names = Vec::new();
    for subtopology in &topology.subtopologies {
        for name in subtopology
            .source_topics
            .iter()
            .chain(&subtopology.repartition_sink_topics)
            .map(|name| name.as_str())
            .chain(
                subtopology
                    .repartition_source_topics
                    .iter()
                    .chain(&subtopology.state_changelog_topics)
                    .map(|topic| topic.name.as_str()),
            )
        {
            if seen.insert(name) {
                names.push(name);
            }
        }
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::messages::streams_group_heartbeat_request::{Subtopology, TopicInfo};
    use kafka_protocol::protocol::StrBytes;

    #[test]
    fn rejects_internal_topics_before_invalid_names_in_request_order() {
        let topology = Topology::default().with_subtopologies(vec![
            Subtopology::default()
                .with_source_topics(names(&["__consumer_offsets", "bad name"]))
                .with_repartition_sink_topics(names(&["__transaction_state"]))
                .with_repartition_source_topics(vec![
                    TopicInfo::default().with_name(name("__share_group_state")),
                ]),
        ]);
        assert_eq!(
            validate(&topology),
            Err("Use of Kafka internal topics __consumer_offsets,__transaction_state,__share_group_state in a Kafka Streams topology is prohibited.".to_owned())
        );
    }

    #[test]
    fn rejects_distinct_invalid_names_in_request_order() {
        let topology = Topology::default().with_subtopologies(vec![
            Subtopology::default()
                .with_source_topics(names(&["a ", "a "]))
                .with_repartition_sink_topics(names(&["b?"]))
                .with_state_changelog_topics(vec![TopicInfo::default().with_name(name("d/"))]),
        ]);
        assert_eq!(
            validate(&topology),
            Err("Topic names a ,b?,d/ are not valid topic names.".to_owned())
        );
    }

    fn names(values: &[&str]) -> Vec<TopicName> {
        values.iter().map(|value| name(value)).collect()
    }

    fn name(value: &str) -> TopicName {
        TopicName::from(StrBytes::from_string(value.to_owned()))
    }
}
