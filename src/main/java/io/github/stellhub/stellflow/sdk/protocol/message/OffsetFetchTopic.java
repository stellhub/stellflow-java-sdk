package io.github.stellhub.stellflow.sdk.protocol.message;

import java.util.List;

/** OffsetFetch topic 请求。 */
public record OffsetFetchTopic(String topic, List<OffsetFetchPartition> partitions) {}
