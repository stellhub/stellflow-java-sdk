package io.github.stellhub.stellflow.sdk.protocol.message;

/** Produce 分区写入数据。 */
public record ProducePartitionData(int partition, byte[] records) {}
