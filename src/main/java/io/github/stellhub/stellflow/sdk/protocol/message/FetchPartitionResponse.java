package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import java.util.List;

/** Fetch 分区响应。 */
public record FetchPartitionResponse(
    int partition,
    ErrorCode errorCode,
    long highWatermark,
    long logStartOffset,
    long lastStableOffset,
    List<AbortedTransaction> abortedTransactions,
    byte[] records) {}
