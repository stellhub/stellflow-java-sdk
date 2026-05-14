package io.github.stellhub.stellflow.sdk.protocol.message;

import java.util.List;

/** ListOffsets topic 响应。 */
public record ListOffsetsTopicResponse(
        String topic, List<ListOffsetsPartitionResponse> partitions) {}
