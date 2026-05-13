package io.github.stellhub.stellflow.sdk.protocol.message;

/** OffsetCommit 分区请求。 */
public record OffsetCommitPartition(int partition, long offset, String metadata) {}
