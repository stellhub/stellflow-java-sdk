package io.github.stellhub.stellflow.sdk.consumer;

import io.github.stellhub.stellflow.sdk.metadata.TopicPartition;
import java.util.Collection;

/** Consumer 分区重平衡监听器。 */
public interface ConsumerRebalanceListener {

    /** 分区被撤销时回调。 */
    void onPartitionsRevoked(Collection<TopicPartition> partitions);

    /** 分区被分配时回调。 */
    void onPartitionsAssigned(Collection<TopicPartition> partitions);

    /** 空监听器。 */
    static ConsumerRebalanceListener noop() {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
        };
    }
}
