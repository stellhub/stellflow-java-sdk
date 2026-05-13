package io.github.stellhub.stellflow.sdk.protocol.message;

import java.util.List;

/** Produce topic 响应。 */
public record ProduceTopicResponse(String topic, List<ProducePartitionResponse> partitions) {}
