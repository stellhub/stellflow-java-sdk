package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import java.util.List;

/** ListOffsets 分区响应。 */
public record ListOffsetsPartitionResponse(
        int partition,
        ErrorCode errorCode,
        int leaderEpoch,
        long timestamp,
        long offset,
        List<Long> offsets) {}
