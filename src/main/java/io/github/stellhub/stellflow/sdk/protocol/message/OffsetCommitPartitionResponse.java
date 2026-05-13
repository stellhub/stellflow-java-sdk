package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;

/** OffsetCommit 分区响应。 */
public record OffsetCommitPartitionResponse(int partition, ErrorCode errorCode) {}
