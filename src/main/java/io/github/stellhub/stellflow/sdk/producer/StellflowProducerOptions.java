package io.github.stellhub.stellflow.sdk.producer;

import java.util.Objects;

/** Producer 行为配置。 */
public record StellflowProducerOptions(
        short acks, int timeoutMs, int maxBatchRecords, ProducerPartitioner partitioner) {

    public static final short DEFAULT_ACKS = -1;
    public static final int DEFAULT_TIMEOUT_MS = 30_000;
    public static final int DEFAULT_MAX_BATCH_RECORDS = 1024;

    /** 创建默认配置。 */
    public static StellflowProducerOptions defaults() {
        return new StellflowProducerOptions(
                DEFAULT_ACKS,
                DEFAULT_TIMEOUT_MS,
                DEFAULT_MAX_BATCH_RECORDS,
                new DefaultProducerPartitioner());
    }

    public StellflowProducerOptions {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive");
        }
        if (maxBatchRecords <= 0) {
            throw new IllegalArgumentException("maxBatchRecords must be positive");
        }
        partitioner = Objects.requireNonNull(partitioner, "partitioner must not be null");
    }

    /** 使用新的 acks 创建配置。 */
    public StellflowProducerOptions withAcks(short value) {
        return new StellflowProducerOptions(value, timeoutMs, maxBatchRecords, partitioner);
    }

    /** 使用新的 timeoutMs 创建配置。 */
    public StellflowProducerOptions withTimeoutMs(int value) {
        return new StellflowProducerOptions(acks, value, maxBatchRecords, partitioner);
    }

    /** 使用新的 maxBatchRecords 创建配置。 */
    public StellflowProducerOptions withMaxBatchRecords(int value) {
        return new StellflowProducerOptions(acks, timeoutMs, value, partitioner);
    }

    /** 使用新的 partitioner 创建配置。 */
    public StellflowProducerOptions withPartitioner(ProducerPartitioner value) {
        return new StellflowProducerOptions(acks, timeoutMs, maxBatchRecords, value);
    }
}
