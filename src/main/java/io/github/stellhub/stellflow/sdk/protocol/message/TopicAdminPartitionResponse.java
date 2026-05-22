package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;

/** Topic 管理分区响应。 */
public record TopicAdminPartitionResponse(int partition, ErrorCode errorCode, int leaderEpoch) {}
