package io.github.stellhub.stellflow.sdk.protocol.message;

import java.util.List;

/** Produce topic 写入数据。 */
public record ProduceTopicData(String topic, List<ProducePartitionData> partitions) {}
