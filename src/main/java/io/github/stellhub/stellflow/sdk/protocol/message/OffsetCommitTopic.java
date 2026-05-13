package io.github.stellhub.stellflow.sdk.protocol.message;

import java.util.List;

/** OffsetCommit topic 请求。 */
public record OffsetCommitTopic(String topic, List<OffsetCommitPartition> partitions) {}
