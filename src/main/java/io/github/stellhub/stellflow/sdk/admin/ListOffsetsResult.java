package io.github.stellhub.stellflow.sdk.admin;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import java.util.List;

/** ListOffsets 查询结果。 */
public record ListOffsetsResult(
        String topic,
        int partition,
        ErrorCode errorCode,
        int leaderEpoch,
        long timestamp,
        long offset,
        List<Long> offsets) {

    public ListOffsetsResult {
        offsets = List.copyOf(offsets);
    }
}
