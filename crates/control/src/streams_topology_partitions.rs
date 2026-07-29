use crate::streams_groups::StreamsTopology;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub(crate) struct StreamsPartitionPlan {
    pub task_counts: Vec<i32>,
    pub internal_counts: BTreeMap<String, i32>,
    pub invalid_topics: BTreeSet<String>,
}

pub(crate) fn derive(
    topology: &StreamsTopology,
    known: &HashMap<&str, i32>,
) -> StreamsPartitionPlan {
    let mut task_counts = topology
        .subtopologies
        .iter()
        .map(|subtopology| {
            subtopology
                .source_topics
                .iter()
                .filter_map(|topic| known.get(topic.as_str()).copied())
                .chain(
                    subtopology
                        .repartition_source_topics
                        .iter()
                        .flat_map(|topic| {
                            [
                                positive(topic.partitions),
                                known.get(topic.name.as_str()).copied(),
                            ]
                        })
                        .flatten(),
                )
                .max()
                .unwrap_or(0)
        })
        .collect::<Vec<_>>();

    loop {
        let sink_counts = repartition_sink_counts(topology, &task_counts);
        let mut changed = false;
        for (index, subtopology) in topology.subtopologies.iter().enumerate() {
            let inherited = subtopology
                .repartition_source_topics
                .iter()
                .filter_map(|topic| sink_counts.get(&topic.name).copied())
                .max()
                .unwrap_or(0);
            if inherited > task_counts[index] {
                task_counts[index] = inherited;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    let sink_counts = repartition_sink_counts(topology, &task_counts);
    let mut internal_counts = BTreeMap::new();
    let mut invalid_topics = BTreeSet::new();

    validate_sink_topics(known, &sink_counts, &mut invalid_topics);
    for (index, subtopology) in topology.subtopologies.iter().enumerate() {
        for topic in &subtopology.repartition_source_topics {
            let candidates = [
                positive(topic.partitions),
                known.get(topic.name.as_str()).copied(),
                sink_counts.get(&topic.name).copied(),
            ];
            merge_candidates(
                &topic.name,
                candidates,
                &mut internal_counts,
                &mut invalid_topics,
            );
        }
        for topic in &subtopology.state_changelog_topics {
            let candidates = [
                positive(topic.partitions),
                known.get(topic.name.as_str()).copied(),
                positive(task_counts[index]),
            ];
            merge_candidates(
                &topic.name,
                candidates,
                &mut internal_counts,
                &mut invalid_topics,
            );
        }
    }

    StreamsPartitionPlan {
        task_counts,
        internal_counts,
        invalid_topics,
    }
}

fn repartition_sink_counts(
    topology: &StreamsTopology,
    task_counts: &[i32],
) -> BTreeMap<String, i32> {
    let mut counts = BTreeMap::<String, i32>::new();
    for (subtopology, task_count) in topology.subtopologies.iter().zip(task_counts) {
        if *task_count <= 0 {
            continue;
        }
        for topic in &subtopology.repartition_sink_topics {
            counts
                .entry(topic.clone())
                .and_modify(|current| *current = (*current).max(*task_count))
                .or_insert(*task_count);
        }
    }
    counts
}

fn validate_sink_topics(
    known: &HashMap<&str, i32>,
    sink_counts: &BTreeMap<String, i32>,
    invalid: &mut BTreeSet<String>,
) {
    for (topic, expected) in sink_counts {
        if known
            .get(topic.as_str())
            .is_some_and(|actual| actual != expected)
        {
            invalid.insert(topic.clone());
        }
    }
}

fn merge_candidates(
    topic: &str,
    candidates: [Option<i32>; 3],
    counts: &mut BTreeMap<String, i32>,
    invalid: &mut BTreeSet<String>,
) {
    let values = candidates.into_iter().flatten().collect::<BTreeSet<_>>();
    if values.len() > 1 {
        invalid.insert(topic.to_owned());
    }
    let Some(partitions) = values.last().copied() else {
        return;
    };
    if let Some(previous) = counts.insert(topic.to_owned(), partitions)
        && previous != partitions
    {
        invalid.insert(topic.to_owned());
    }
}

fn positive(value: i32) -> Option<i32> {
    (value > 0).then_some(value)
}
