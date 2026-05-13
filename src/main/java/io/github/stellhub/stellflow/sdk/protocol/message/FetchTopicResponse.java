package io.github.stellhub.stellflow.sdk.protocol.message;

import java.util.List;

/** Fetch topic 响应。 */
public record FetchTopicResponse(String topic, List<FetchPartitionResponse> partitions) {}
