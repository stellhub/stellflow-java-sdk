package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;

/** Produce 分区响应。 */
public record ProducePartitionResponse(
        int partition,
        ErrorCode errorCode,
        long baseOffset,
        int currentLeaderEpoch,
        long logAppendTimeMs,
        long logStartOffset) {}
