use kafka_protocol::messages::describe_configs_response::DescribeConfigsSynonym;
use kafka_protocol::protocol::StrBytes;

const DYNAMIC_TOPIC_CONFIG: i8 = 1;
const DYNAMIC_DEFAULT_BROKER_CONFIG: i8 = 3;
const STATIC_BROKER_CONFIG: i8 = 4;
const DEFAULT_CONFIG: i8 = 5;

pub(super) fn same_name(name: &str, value: &str, source: i8) -> Vec<DescribeConfigsSynonym> {
    vec![synonym(name, value, source)]
}

pub(super) fn topic(
    name: &str,
    value: &str,
    default_value: &str,
    dynamic: bool,
) -> Vec<DescribeConfigsSynonym> {
    let mut synonyms = Vec::with_capacity(if dynamic { 2 } else { 1 });
    if dynamic {
        synonyms.push(synonym(name, value, DYNAMIC_TOPIC_CONFIG));
    }
    synonyms.push(synonym(
        topic_default_name(name),
        default_value,
        DEFAULT_CONFIG,
    ));
    synonyms
}

pub(super) fn dynamic_default_broker(
    name: &str,
    value: &str,
    static_value: &str,
) -> Vec<DescribeConfigsSynonym> {
    vec![
        synonym(name, value, DYNAMIC_DEFAULT_BROKER_CONFIG),
        synonym(name, static_value, STATIC_BROKER_CONFIG),
    ]
}

fn synonym(name: &str, value: &str, source: i8) -> DescribeConfigsSynonym {
    DescribeConfigsSynonym::default()
        .with_name(StrBytes::from_string(name.to_owned()))
        .with_value(Some(StrBytes::from_string(value.to_owned())))
        .with_source(source)
}

fn topic_default_name(name: &str) -> &str {
    match name {
        "cleanup.policy" => "log.cleanup.policy",
        "retention.ms" => "log.retention.ms",
        "retention.bytes" => "log.retention.bytes",
        "file.delete.delay.ms" => "log.segment.delete.delay.ms",
        "flush.messages" => "log.flush.interval.messages",
        "flush.ms" => "log.flush.interval.ms",
        "delete.retention.ms" => "log.cleaner.delete.retention.ms",
        "min.compaction.lag.ms" => "log.cleaner.min.compaction.lag.ms",
        "max.compaction.lag.ms" => "log.cleaner.max.compaction.lag.ms",
        "min.cleanable.dirty.ratio" => "log.cleaner.min.cleanable.ratio",
        "max.message.bytes" => "message.max.bytes",
        "message.timestamp.type" => "log.message.timestamp.type",
        "message.timestamp.before.max.ms" => "log.message.timestamp.before.max.ms",
        "message.timestamp.after.max.ms" => "log.message.timestamp.after.max.ms",
        _ => name,
    }
}
