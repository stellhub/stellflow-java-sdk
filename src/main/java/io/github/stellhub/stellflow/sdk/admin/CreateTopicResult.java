package io.github.stellhub.stellflow.sdk.admin;

import java.util.List;

/** Topic 创建结果。 */
public record CreateTopicResult(
        String topic, boolean created, List<CreateTopicPartitionResult> partitions) {

    public CreateTopicResult {
        partitions = List.copyOf(partitions);
    }
}
