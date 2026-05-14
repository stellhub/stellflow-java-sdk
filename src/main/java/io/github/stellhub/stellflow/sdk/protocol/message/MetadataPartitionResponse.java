package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import java.util.List;

/** Partition 元数据响应。 */
public record MetadataPartitionResponse(
        ErrorCode errorCode,
        int partition,
        int leaderId,
        int leaderEpoch,
        List<Integer> replicaNodes,
        List<Integer> isrNodes,
        List<Integer> offlineReplicaNodes) {}
