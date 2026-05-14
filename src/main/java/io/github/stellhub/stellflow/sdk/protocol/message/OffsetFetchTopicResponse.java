package io.github.stellhub.stellflow.sdk.protocol.message;

import java.util.List;

/** OffsetFetch topic 响应。 */
public record OffsetFetchTopicResponse(
        String topic, List<OffsetFetchPartitionResponse> partitions) {}
