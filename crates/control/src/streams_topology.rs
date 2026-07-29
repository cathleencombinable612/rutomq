use crate::streams_groups::{
    STREAMS_STATUS_INCORRECTLY_PARTITIONED_TOPICS, STREAMS_STATUS_MISSING_INTERNAL_TOPICS,
    STREAMS_STATUS_MISSING_SOURCE_TOPICS, StreamsGroupStatus, StreamsInternalTopicRequirement,
    StreamsSubtopology, StreamsTaskId, StreamsTopology,
};
use crate::streams_topology_partitions;
use crate::{ControlError, TopicInfo};
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, Clone)]
pub(crate) struct ResolvedStreamsTopology {
    pub topology: StreamsTopology,
    pub tasks: Vec<StreamsTaskId>,
    pub statuses: Vec<StreamsGroupStatus>,
}

impl ResolvedStreamsTopology {
    pub fn ready(&self) -> bool {
        self.statuses.is_empty()
    }
}

pub(crate) fn resolve(
    topology: &StreamsTopology,
    topics: &[TopicInfo],
) -> Result<ResolvedStreamsTopology, ControlError> {
    let known = topics
        .iter()
        .map(|topic| (topic.name.as_str(), topic.partitions))
        .collect::<HashMap<_, _>>();
    let mut resolved = topology.clone();
    let mut tasks = Vec::new();
    let mut missing_sources = BTreeSet::new();
    let mut missing_internal = BTreeSet::new();
    let mut incorrectly_partitioned = BTreeSet::new();

    for subtopology in &mut resolved.subtopologies {
        resolve_regex_sources(subtopology, topics, &mut missing_sources)?;
        collect_missing_sources(subtopology, &known, &mut missing_sources);
        collect_missing_internal(subtopology, &known, &mut missing_internal);
    }
    let plan = streams_topology_partitions::derive(&resolved, &known);
    incorrectly_partitioned.extend(plan.invalid_topics);

    for (subtopology, task_count) in resolved
        .subtopologies
        .iter_mut()
        .zip(plan.task_counts.iter().copied())
    {
        let source_counts = source_partition_counts(subtopology, &known);
        validate_copartitioning(
            subtopology,
            &known,
            &source_counts,
            &plan.internal_counts,
            &mut incorrectly_partitioned,
        );

        for topic in &mut subtopology.state_changelog_topics {
            topic.partitions = task_count;
        }
        for topic in &mut subtopology.repartition_source_topics {
            if let Some(partitions) = plan.internal_counts.get(&topic.name) {
                topic.partitions = *partitions;
            }
        }
        for partition in 0..task_count {
            tasks.push(StreamsTaskId {
                subtopology_id: subtopology.subtopology_id.clone(),
                partition,
            });
        }
    }
    tasks.sort();

    let mut statuses = Vec::new();
    if !missing_sources.is_empty() {
        push_status(
            &mut statuses,
            STREAMS_STATUS_MISSING_SOURCE_TOPICS,
            "missing source topics",
            missing_sources,
        );
    } else if !incorrectly_partitioned.is_empty() {
        push_status(
            &mut statuses,
            STREAMS_STATUS_INCORRECTLY_PARTITIONED_TOPICS,
            "incorrectly partitioned topics",
            incorrectly_partitioned,
        );
    } else {
        push_status(
            &mut statuses,
            STREAMS_STATUS_MISSING_INTERNAL_TOPICS,
            "missing internal topics",
            missing_internal,
        );
    }
    Ok(ResolvedStreamsTopology {
        topology: resolved,
        tasks,
        statuses,
    })
}

pub fn streams_topology_topic_names(
    topology: &StreamsTopology,
    topics: &[TopicInfo],
) -> Result<BTreeSet<String>, ControlError> {
    let resolved = resolve(topology, topics)?;
    let mut names = BTreeSet::new();
    for subtopology in resolved.topology.subtopologies {
        names.extend(subtopology.source_topics);
        names.extend(subtopology.repartition_sink_topics);
        names.extend(
            subtopology
                .state_changelog_topics
                .into_iter()
                .chain(subtopology.repartition_source_topics)
                .map(|topic| topic.name),
        );
    }
    Ok(names)
}

pub fn streams_internal_topic_requirements(
    topology: &StreamsTopology,
    topics: &[TopicInfo],
) -> Result<Vec<StreamsInternalTopicRequirement>, ControlError> {
    let resolved = resolve(topology, topics)?;
    if resolved.statuses.iter().any(|status| {
        matches!(
            status.code,
            STREAMS_STATUS_MISSING_SOURCE_TOPICS | STREAMS_STATUS_INCORRECTLY_PARTITIONED_TOPICS
        )
    }) {
        return Ok(Vec::new());
    }
    let mut requirements = BTreeMap::<String, StreamsInternalTopicRequirement>::new();
    for subtopology in resolved.topology.subtopologies {
        for topic in subtopology
            .state_changelog_topics
            .into_iter()
            .chain(subtopology.repartition_source_topics)
        {
            if topic.partitions <= 0 {
                continue;
            }
            let requirement = StreamsInternalTopicRequirement {
                topic: topic.clone(),
                partitions: topic.partitions,
            };
            if let Some(existing) = requirements.insert(topic.name.clone(), requirement.clone())
                && existing != requirement
            {
                return Err(ControlError::InvalidRequest(format!(
                    "streams internal topic {} has conflicting requirements",
                    topic.name
                )));
            }
        }
    }
    Ok(requirements.into_values().collect())
}

fn resolve_regex_sources(
    subtopology: &mut StreamsSubtopology,
    topics: &[TopicInfo],
    missing: &mut BTreeSet<String>,
) -> Result<(), ControlError> {
    let mut names = subtopology
        .source_topics
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for pattern in &subtopology.source_topic_regex {
        let expression = Regex::new(&format!("^(?:{pattern})$")).map_err(|error| {
            ControlError::InvalidRequest(format!("invalid streams source topic regex: {error}"))
        })?;
        let matches = topics
            .iter()
            .filter(|topic| expression.is_match(&topic.name))
            .map(|topic| topic.name.clone())
            .collect::<Vec<_>>();
        if matches.is_empty() {
            missing.insert(format!("regex:{pattern}"));
        }
        names.extend(matches);
    }
    subtopology.source_topics = names.into_iter().collect();
    Ok(())
}

fn collect_missing_sources(
    subtopology: &StreamsSubtopology,
    known: &HashMap<&str, i32>,
    missing: &mut BTreeSet<String>,
) {
    for topic in &subtopology.source_topics {
        if !known.contains_key(topic.as_str()) {
            missing.insert(topic.clone());
        }
    }
}

fn collect_missing_internal(
    subtopology: &StreamsSubtopology,
    known: &HashMap<&str, i32>,
    missing: &mut BTreeSet<String>,
) {
    for topic in subtopology
        .repartition_source_topics
        .iter()
        .chain(subtopology.state_changelog_topics.iter())
    {
        if !known.contains_key(topic.name.as_str()) {
            missing.insert(topic.name.clone());
        }
    }
}

fn source_partition_counts(
    subtopology: &StreamsSubtopology,
    known: &HashMap<&str, i32>,
) -> BTreeMap<String, i32> {
    subtopology
        .source_topics
        .iter()
        .filter_map(|topic| {
            known
                .get(topic.as_str())
                .map(|partitions| (topic.clone(), *partitions))
        })
        .collect()
}

fn validate_copartitioning(
    subtopology: &StreamsSubtopology,
    known: &HashMap<&str, i32>,
    source_counts: &BTreeMap<String, i32>,
    internal_counts: &BTreeMap<String, i32>,
    invalid: &mut BTreeSet<String>,
) {
    for group in &subtopology.copartition_groups {
        let mut entries = Vec::new();
        for index in &group.source_topics {
            if let Some(name) = indexed(&subtopology.source_topics, *index) {
                entries.push((name.clone(), source_counts.get(name).copied()));
            }
        }
        for index in &group.source_topic_regex {
            if let Some(pattern) = indexed(&subtopology.source_topic_regex, *index)
                && let Ok(expression) = Regex::new(&format!("^(?:{pattern})$"))
            {
                entries.extend(
                    known
                        .iter()
                        .filter(|(name, _)| expression.is_match(name))
                        .map(|(name, partitions)| ((*name).to_owned(), Some(*partitions))),
                );
            }
        }
        for index in &group.repartition_source_topics {
            if let Some(topic) = indexed(&subtopology.repartition_source_topics, *index) {
                entries.push((
                    topic.name.clone(),
                    internal_counts.get(&topic.name).copied(),
                ));
            }
        }
        let expected = entries.iter().find_map(|(_, partitions)| *partitions);
        if let Some(expected) = expected {
            invalid.extend(
                entries
                    .into_iter()
                    .filter(|(_, partitions)| partitions.is_some_and(|value| value != expected))
                    .map(|(name, _)| name),
            );
        }
    }
}

fn indexed<T>(values: &[T], index: i16) -> Option<&T> {
    usize::try_from(index)
        .ok()
        .and_then(|index| values.get(index))
}

fn push_status(
    statuses: &mut Vec<StreamsGroupStatus>,
    code: i8,
    prefix: &str,
    topics: BTreeSet<String>,
) {
    if topics.is_empty() {
        return;
    }
    statuses.push(StreamsGroupStatus {
        code,
        detail: format!(
            "{prefix}: {}",
            topics.into_iter().collect::<Vec<_>>().join(",")
        ),
    });
}
