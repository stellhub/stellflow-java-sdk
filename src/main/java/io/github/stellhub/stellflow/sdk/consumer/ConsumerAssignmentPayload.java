package io.github.stellhub.stellflow.sdk.consumer;

import io.github.stellhub.stellflow.sdk.metadata.TopicPartition;
import java.util.List;

/** Consumer 分区分配 payload。 */
public record ConsumerAssignmentPayload(List<TopicPartition> partitions) {

    public ConsumerAssignmentPayload {
        partitions = List.copyOf(partitions == null ? List.of() : partitions);
    }
}
