package io.github.stellhub.stellflow.sdk.admin;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;

/** Topic 创建分区结果。 */
public record CreateTopicPartitionResult(int partition, ErrorCode errorCode, int leaderEpoch) {}
