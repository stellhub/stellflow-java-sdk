package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;

/** OffsetFetch 分区响应。 */
public record OffsetFetchPartitionResponse(
    int partition, long offset, String metadata, ErrorCode errorCode) {}
