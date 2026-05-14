package io.github.stellhub.stellflow.sdk.producer;

import java.util.List;

/** Producer 分区选择器。 */
@FunctionalInterface
public interface ProducerPartitioner {

  /** 选择目标分区。 */
  int partition(String topic, byte[] key, byte[] value, List<Integer> partitions);
}
