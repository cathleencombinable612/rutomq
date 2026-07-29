package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeRequestData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeRequestData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.ShareGroupDescribeRequest;
import org.apache.kafka.common.requests.StreamsGroupDescribeRequest;

public final class GroupDescribeDuplicateSmoke {
    private GroupDescribeDuplicateSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "GroupDescribeDuplicateSmoke <bootstrap> <group-prefix>");
        }
        List<String> groups =
                List.of(args[1] + "-a", args[1] + "-b", args[1] + "-b", args[1] + "-a");

        DescribeGroupsRequest classic =
                new DescribeGroupsRequest.Builder(
                                new DescribeGroupsRequestData().setGroups(groups))
                        .build((short) 6);
        ByteBuffer body = KafkaWire.exchange(args[0], classic, 7731);
        DescribeGroupsResponseData classicResponse =
                new DescribeGroupsResponseData(new ByteBufferAccessor(body), (short) 6);
        assertGroups(
                "DescribeGroups",
                groups,
                classicResponse.groups().stream()
                        .map(group -> new GroupResult(group.groupId(), group.errorCode()))
                        .toList());

        ConsumerGroupDescribeRequest consumer =
                new ConsumerGroupDescribeRequest.Builder(
                                new ConsumerGroupDescribeRequestData().setGroupIds(groups))
                        .build((short) 1);
        body = KafkaWire.exchange(args[0], consumer, 7732);
        ConsumerGroupDescribeResponseData consumerResponse =
                new ConsumerGroupDescribeResponseData(
                        new ByteBufferAccessor(body), (short) 1);
        assertGroups(
                "ConsumerGroupDescribe",
                groups,
                consumerResponse.groups().stream()
                        .map(group -> new GroupResult(group.groupId(), group.errorCode()))
                        .toList());

        StreamsGroupDescribeRequest streams =
                new StreamsGroupDescribeRequest.Builder(
                                new StreamsGroupDescribeRequestData().setGroupIds(groups))
                        .build((short) 0);
        body = KafkaWire.exchange(args[0], streams, 7733);
        StreamsGroupDescribeResponseData streamsResponse =
                new StreamsGroupDescribeResponseData(
                        new ByteBufferAccessor(body), (short) 0);
        assertGroups(
                "StreamsGroupDescribe",
                groups,
                streamsResponse.groups().stream()
                        .map(group -> new GroupResult(group.groupId(), group.errorCode()))
                        .toList());

        ShareGroupDescribeRequest share =
                new ShareGroupDescribeRequest.Builder(
                                new ShareGroupDescribeRequestData().setGroupIds(groups))
                        .build((short) 1);
        body = KafkaWire.exchange(args[0], share, 7734);
        ShareGroupDescribeResponseData shareResponse =
                new ShareGroupDescribeResponseData(
                        new ByteBufferAccessor(body), (short) 1);
        assertGroups(
                "ShareGroupDescribe",
                groups,
                shareResponse.groups().stream()
                        .map(group -> new GroupResult(group.groupId(), group.errorCode()))
                        .toList());

        System.out.printf(
                "Kafka 4.2 group describe duplicate cardinality passed prefix=%s%n",
                args[1]);
    }

    private static void assertGroups(
            String api, List<String> expectedIds, List<GroupResult> groups) {
        if (groups.size() != expectedIds.size()) {
            throw new AssertionError(api + " returned " + groups.size() + " results: " + groups);
        }
        for (int index = 0; index < expectedIds.size(); index++) {
            GroupResult group = groups.get(index);
            if (!expectedIds.get(index).equals(group.groupId())
                    || group.errorCode() != Errors.GROUP_ID_NOT_FOUND.code()) {
                throw new AssertionError(
                        api + " result " + index + " did not preserve the request: " + groups);
            }
        }
    }

    private record GroupResult(String groupId, short errorCode) {}
}
