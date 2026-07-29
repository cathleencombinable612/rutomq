#!/usr/bin/env python3

import sys
import time

from confluent_kafka import Consumer, KafkaError, Producer, libversion, version
from confluent_kafka.admin import AdminClient, NewTopic


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def create_topics(bootstrap: str, data_topic: str, transaction_topic: str) -> None:
    admin = AdminClient(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "rutomq-librdkafka-admin",
            "socket.timeout.ms": 10_000,
        }
    )
    futures = admin.create_topics(
        [
            NewTopic(data_topic, num_partitions=2, replication_factor=1),
            NewTopic(transaction_topic, num_partitions=1, replication_factor=1),
        ],
        request_timeout=15,
    )
    for topic, future in futures.items():
        future.result(15)
        metadata = admin.list_topics(topic=topic, timeout=15)
        require(topic in metadata.topics, f"Admin metadata omitted {topic}")
        expected_partitions = 1 if topic == transaction_topic else 2
        require(
            len(metadata.topics[topic].partitions) == expected_partitions,
            f"unexpected partition count for {topic}",
        )


def producer(bootstrap: str, client_id: str, **extra: object) -> Producer:
    config: dict[str, object] = {
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": 15_000,
    }
    config.update(extra)
    return Producer(config)


def produce_records(
    client: Producer,
    topic: str,
    values: list[str],
    partitions: int,
) -> list[tuple[int, int]]:
    delivered: list[tuple[int, int]] = []

    def delivery(error: object, message: object) -> None:
        if error is not None:
            raise AssertionError(f"librdkafka delivery failed: {error}")
        delivered.append((message.partition(), message.offset()))

    for index, value in enumerate(values):
        client.produce(
            topic,
            partition=index % partitions,
            key=f"key-{index}".encode(),
            value=value.encode(),
            headers=[("source", b"librdkafka"), ("ordinal", str(index).encode())],
            on_delivery=delivery,
        )
    require(client.flush(15) == 0, "librdkafka producer did not flush")
    require(len(delivered) == len(values), "missing librdkafka delivery callbacks")
    return delivered


def consumer(
    bootstrap: str,
    group: str,
    isolation: str = "read_committed",
) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": f"{group}-client",
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": isolation,
            "session.timeout.ms": 10_000,
        }
    )


def consume_values(
    client: Consumer,
    topic: str,
    expected_count: int,
    timeout: float = 20,
) -> list[str]:
    client.subscribe([topic])
    deadline = time.monotonic() + timeout
    values: list[str] = []
    while len(values) < expected_count and time.monotonic() < deadline:
        message = client.poll(0.5)
        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise AssertionError(f"librdkafka consume failed: {message.error()}")
        values.append(message.value().decode())
    require(
        len(values) == expected_count,
        f"expected {expected_count} records from {topic}, got {values}",
    )
    return values


def assert_no_record(client: Consumer, timeout: float = 2) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        message = client.poll(0.2)
        if message is None:
            continue
        if message.error() and message.error().code() == KafkaError._PARTITION_EOF:
            continue
        if message.error():
            raise AssertionError(f"librdkafka consume failed: {message.error()}")
        raise AssertionError(f"unexpected read_committed value {message.value()!r}")


def verify_group_resume(bootstrap: str, topic: str, prefix: str) -> None:
    values = [f"{prefix}-data-{index}" for index in range(6)]
    client = producer(bootstrap, f"{prefix}-producer")
    delivered = produce_records(client, topic, values, 2)
    by_partition: dict[int, list[int]] = {0: [], 1: []}
    for partition, offset in delivered:
        by_partition[partition].append(offset)
    require(
        all(sorted(offsets) == [0, 1, 2] for offsets in by_partition.values()),
        f"unexpected idempotent Produce offsets {by_partition}",
    )

    group = f"{prefix}-group"
    first = consumer(bootstrap, group)
    consumed = consume_values(first, topic, len(values))
    require(set(consumed) == set(values), f"librdkafka payload mismatch {consumed}")
    first.commit(asynchronous=False)
    first.close()

    resume_value = f"{prefix}-resume"
    produce_records(client, topic, [resume_value], 1)
    second = consumer(bootstrap, group)
    resumed = consume_values(second, topic, 1)
    require(resumed == [resume_value], f"group replayed committed data {resumed}")
    second.commit(asynchronous=False)
    second.close()


def verify_transactions(
    bootstrap: str,
    topic: str,
    prefix: str,
) -> None:
    client = producer(
        bootstrap,
        f"{prefix}-transactional-producer",
        **{"transactional.id": f"{prefix}-transactional-id"},
    )
    client.init_transactions(15)

    committed = f"{prefix}-committed"
    client.begin_transaction()
    produce_records(client, topic, [committed], 1)
    client.commit_transaction(15)

    aborted = f"{prefix}-aborted"
    client.begin_transaction()
    produce_records(client, topic, [aborted], 1)
    client.abort_transaction(15)

    read_committed = consumer(
        bootstrap,
        f"{prefix}-read-committed",
        "read_committed",
    )
    visible = consume_values(read_committed, topic, 1)
    require(visible == [committed], f"committed transaction mismatch {visible}")
    assert_no_record(read_committed)
    read_committed.close()

    read_uncommitted = consumer(
        bootstrap,
        f"{prefix}-read-uncommitted",
        "read_uncommitted",
    )
    all_values = consume_values(read_uncommitted, topic, 2)
    require(
        all_values == [committed, aborted],
        f"read_uncommitted transaction mismatch {all_values}",
    )
    read_uncommitted.close()


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("librdkafka_smoke.py <bootstrap> <prefix>")
    bootstrap, prefix = sys.argv[1:]
    data_topic = f"{prefix}-data"
    transaction_topic = f"{prefix}-transactions"
    create_topics(bootstrap, data_topic, transaction_topic)
    verify_group_resume(bootstrap, data_topic, prefix)
    verify_transactions(bootstrap, transaction_topic, prefix)
    print(
        "librdkafka acceptance passed "
        f"confluent-kafka={version()[0]} librdkafka={libversion()[0]} prefix={prefix}"
    )


if __name__ == "__main__":
    main()
