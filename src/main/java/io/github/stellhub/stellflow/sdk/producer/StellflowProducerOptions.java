package io.github.stellhub.stellflow.sdk.producer;

import java.util.Objects;

/** Producer 行为配置。 */
public record StellflowProducerOptions(
        short acks,
        int timeoutMs,
        int maxBatchRecords,
        ProducerPartitioner partitioner,
        boolean autoCreateTopics,
        int autoCreateTopicPartitionCount) {

    public static final short DEFAULT_ACKS = -1;
    public static final int DEFAULT_TIMEOUT_MS = 30_000;
    public static final int DEFAULT_MAX_BATCH_RECORDS = 1024;
    public static final boolean DEFAULT_AUTO_CREATE_TOPICS = true;
    public static final int DEFAULT_AUTO_CREATE_TOPIC_PARTITION_COUNT = 2;

    /** 创建默认配置。 */
    public static StellflowProducerOptions defaults() {
        return new StellflowProducerOptions(
                DEFAULT_ACKS,
                DEFAULT_TIMEOUT_MS,
                DEFAULT_MAX_BATCH_RECORDS,
                new DefaultProducerPartitioner(),
                DEFAULT_AUTO_CREATE_TOPICS,
                DEFAULT_AUTO_CREATE_TOPIC_PARTITION_COUNT);
    }

    public StellflowProducerOptions {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive");
        }
        if (maxBatchRecords <= 0) {
            throw new IllegalArgumentException("maxBatchRecords must be positive");
        }
        if (autoCreateTopicPartitionCount <= 0) {
            throw new IllegalArgumentException("autoCreateTopicPartitionCount must be positive");
        }
        partitioner = Objects.requireNonNull(partitioner, "partitioner must not be null");
    }

    /** 使用新的 acks 创建配置。 */
    public StellflowProducerOptions withAcks(short value) {
        return new StellflowProducerOptions(
                value,
                timeoutMs,
                maxBatchRecords,
                partitioner,
                autoCreateTopics,
                autoCreateTopicPartitionCount);
    }

    /** 使用新的 timeoutMs 创建配置。 */
    public StellflowProducerOptions withTimeoutMs(int value) {
        return new StellflowProducerOptions(
                acks, value, maxBatchRecords, partitioner, autoCreateTopics, autoCreateTopicPartitionCount);
    }

    /** 使用新的 maxBatchRecords 创建配置。 */
    public StellflowProducerOptions withMaxBatchRecords(int value) {
        return new StellflowProducerOptions(
                acks, timeoutMs, value, partitioner, autoCreateTopics, autoCreateTopicPartitionCount);
    }

    /** 使用新的 partitioner 创建配置。 */
    public StellflowProducerOptions withPartitioner(ProducerPartitioner value) {
        return new StellflowProducerOptions(
                acks, timeoutMs, maxBatchRecords, value, autoCreateTopics, autoCreateTopicPartitionCount);
    }

    /** 使用新的自动创建 Topic 开关创建配置。 */
    public StellflowProducerOptions withAutoCreateTopics(boolean value) {
        return new StellflowProducerOptions(
                acks, timeoutMs, maxBatchRecords, partitioner, value, autoCreateTopicPartitionCount);
    }

    /** 使用新的自动创建 Topic 分区数创建配置。 */
    public StellflowProducerOptions withAutoCreateTopicPartitionCount(int value) {
        return new StellflowProducerOptions(
                acks, timeoutMs, maxBatchRecords, partitioner, autoCreateTopics, value);
    }
}
