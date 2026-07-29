package main

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	if len(os.Args) != 3 {
		panic("franz-smoke <bootstrap> <prefix>")
	}
	bootstrap, prefix := os.Args[1], os.Args[2]
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dataTopic := prefix + "-data"
	transactionTopic := prefix + "-transactions"
	createTopics(ctx, bootstrap, dataTopic, transactionTopic)
	verifyGroupResume(ctx, bootstrap, dataTopic, prefix)
	verifyTransactions(ctx, bootstrap, transactionTopic, prefix)
	fmt.Printf(
		"franz-go acceptance passed franz-go=%s prefix=%s\n",
		moduleVersion("github.com/twmb/franz-go"),
		prefix,
	)
}

func createTopics(ctx context.Context, bootstrap, dataTopic, transactionTopic string) {
	client := newClient(bootstrap, "rutomq-franz-admin")
	defer client.Close()
	request := &kmsg.CreateTopicsRequest{
		Topics: []kmsg.CreateTopicsRequestTopic{
			{
				Topic:             dataTopic,
				NumPartitions:     2,
				ReplicationFactor: 1,
			},
			{
				Topic:             transactionTopic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
		TimeoutMillis: 15_000,
	}
	raw, err := client.Request(ctx, request)
	must(err)
	response := raw.(*kmsg.CreateTopicsResponse)
	for _, topic := range response.Topics {
		require(topic.ErrorCode == 0, "CreateTopics %s failed with %d", topic.Topic, topic.ErrorCode)
	}

	metadataTopic := dataTopic
	raw, err = client.Request(ctx, &kmsg.MetadataRequest{
		Topics: []kmsg.MetadataRequestTopic{{Topic: &metadataTopic}},
	})
	must(err)
	metadata := raw.(*kmsg.MetadataResponse)
	require(len(metadata.Topics) == 1, "metadata returned %d topics", len(metadata.Topics))
	require(
		metadata.Topics[0].ErrorCode == 0 && len(metadata.Topics[0].Partitions) == 2,
		"unexpected metadata for %s",
		dataTopic,
	)
}

func verifyGroupResume(ctx context.Context, bootstrap, topic, prefix string) {
	values := make([]string, 6)
	records := make([]*kgo.Record, 6)
	for index := range records {
		values[index] = fmt.Sprintf("%s-data-%d", prefix, index)
		records[index] = &kgo.Record{
			Topic:     topic,
			Partition: int32(index % 2),
			Key:       []byte(fmt.Sprintf("key-%d", index)),
			Value:     []byte(values[index]),
			Headers: []kgo.RecordHeader{
				{Key: "source", Value: []byte("franz-go")},
				{Key: "ordinal", Value: []byte(fmt.Sprint(index))},
			},
		}
	}
	producer := newProducer(bootstrap, prefix+"-producer")
	defer producer.Close()
	results := producer.ProduceSync(ctx, records...)
	offsets := map[int32][]int64{0: {}, 1: {}}
	for _, result := range results {
		must(result.Err)
		offsets[result.Record.Partition] = append(
			offsets[result.Record.Partition],
			result.Record.Offset,
		)
	}
	for partition, found := range offsets {
		sort.Slice(found, func(left, right int) bool { return found[left] < found[right] })
		require(
			fmt.Sprint(found) == "[0 1 2]",
			"unexpected partition %d offsets %v",
			partition,
			found,
		)
	}

	group := prefix + "-group"
	first := newConsumer(bootstrap, group, topic, kgo.ReadCommitted())
	consumed := consumeExact(ctx, first, len(values))
	require(sameValues(consumed, values), "franz-go payload mismatch %v", consumed)
	must(first.CommitRecords(ctx, consumed...))
	first.Close()

	resumeValue := prefix + "-resume"
	result := producer.ProduceSync(ctx, &kgo.Record{
		Topic:     topic,
		Partition: 0,
		Value:     []byte(resumeValue),
	})
	must(result.FirstErr())

	second := newConsumer(bootstrap, group, topic, kgo.ReadCommitted())
	resumed := consumeExact(ctx, second, 1)
	require(
		string(resumed[0].Value) == resumeValue,
		"group replayed committed data %q",
		resumed[0].Value,
	)
	must(second.CommitRecords(ctx, resumed...))
	second.Close()
}

func verifyTransactions(ctx context.Context, bootstrap, topic, prefix string) {
	transactional, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ClientID(prefix+"-transactional-producer"),
		kgo.TransactionalID(prefix+"-transactional-id"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RequestTimeoutOverhead(15*time.Second),
	)
	must(err)
	defer transactional.Close()

	committed := prefix + "-committed"
	must(transactional.BeginTransaction())
	must(transactional.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Value: []byte(committed),
	}).FirstErr())
	must(transactional.EndTransaction(ctx, kgo.TryCommit))

	aborted := prefix + "-aborted"
	must(transactional.BeginTransaction())
	must(transactional.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Value: []byte(aborted),
	}).FirstErr())
	must(transactional.EndTransaction(ctx, kgo.TryAbort))

	readCommitted := newConsumer(
		bootstrap,
		prefix+"-read-committed",
		topic,
		kgo.ReadCommitted(),
	)
	visible := consumeExact(ctx, readCommitted, 1)
	require(
		string(visible[0].Value) == committed,
		"committed transaction mismatch %q",
		visible[0].Value,
	)
	assertNoRecord(readCommitted, 2*time.Second)
	readCommitted.Close()

	readUncommitted := newConsumer(
		bootstrap,
		prefix+"-read-uncommitted",
		topic,
		kgo.ReadUncommitted(),
	)
	all := consumeExact(ctx, readUncommitted, 2)
	require(
		string(all[0].Value) == committed && string(all[1].Value) == aborted,
		"read_uncommitted transaction mismatch %q %q",
		all[0].Value,
		all[1].Value,
	)
	readUncommitted.Close()
}

func newClient(bootstrap, clientID string) *kgo.Client {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ClientID(clientID),
		kgo.RequestTimeoutOverhead(15*time.Second),
	)
	must(err)
	return client
}

func newProducer(bootstrap, clientID string) *kgo.Client {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ClientID(clientID),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RequestTimeoutOverhead(15*time.Second),
	)
	must(err)
	return client
}

func newConsumer(
	bootstrap, group, topic string,
	isolation kgo.IsolationLevel,
) *kgo.Client {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ClientID(group+"-client"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(isolation),
		kgo.RequestTimeoutOverhead(15*time.Second),
	)
	must(err)
	return client
}

func consumeExact(ctx context.Context, client *kgo.Client, count int) []*kgo.Record {
	records := make([]*kgo.Record, 0, count)
	for len(records) < count {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			panic(err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if len(records) < count {
				records = append(records, record)
			}
		})
	}
	return records
}

func assertNoRecord(client *kgo.Client, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	fetches := client.PollFetches(ctx)
	if fetches.NumRecords() != 0 {
		panic(fmt.Sprintf("read_committed exposed %d unexpected records", fetches.NumRecords()))
	}
}

func sameValues(records []*kgo.Record, expected []string) bool {
	found := make([]string, 0, len(records))
	for _, record := range records {
		found = append(found, string(record.Value))
	}
	sort.Strings(found)
	sort.Strings(expected)
	return fmt.Sprint(found) == fmt.Sprint(expected)
}

func moduleVersion(path string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dependency := range info.Deps {
		if dependency.Path == path {
			return dependency.Version
		}
	}
	return "unknown"
}

func require(condition bool, format string, args ...any) {
	if !condition {
		panic(fmt.Sprintf(format, args...))
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
