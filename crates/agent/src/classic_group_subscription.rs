use bytes::{Buf, Bytes};
use kafka_protocol::messages::ConsumerProtocolSubscription;
use kafka_protocol::protocol::Decodable;
use std::collections::BTreeSet;

pub(super) fn subscribed_topics(
    protocol_type: &str,
    protocols: &[(String, Vec<u8>)],
) -> Vec<String> {
    if protocol_type != "consumer" {
        return Vec::new();
    }
    protocols
        .iter()
        .filter_map(|(_, metadata)| decode_subscription(metadata))
        .flatten()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn decode_subscription(metadata: &[u8]) -> Option<Vec<String>> {
    let mut metadata = Bytes::copy_from_slice(metadata);
    if metadata.remaining() < 2 {
        return None;
    }
    let version = metadata.get_i16();
    let subscription = ConsumerProtocolSubscription::decode(&mut metadata, version).ok()?;
    Some(
        subscription
            .topics
            .into_iter()
            .map(|topic| topic.as_str().to_owned())
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use kafka_protocol::protocol::Encodable;
    use kafka_protocol::protocol::StrBytes;

    #[test]
    fn decodes_join_group_consumer_subscription() {
        let subscription = ConsumerProtocolSubscription::default().with_topics(vec![
            StrBytes::from_string("events".to_owned()),
            StrBytes::from_string("orders".to_owned()),
        ]);
        let mut metadata = BytesMut::new();
        metadata.put_i16(3);
        subscription.encode(&mut metadata, 3).unwrap();
        let protocols = vec![("range".to_owned(), metadata.to_vec())];
        assert_eq!(
            subscribed_topics("consumer", &protocols),
            ["events".to_owned(), "orders".to_owned()]
        );
    }
}
