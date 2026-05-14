package io.github.stellhub.stellflow.sdk.admin;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import java.util.List;

/** 分区描述结果。 */
public record PartitionDescription(
    int partition,
    ErrorCode errorCode,
    int leaderId,
    int leaderEpoch,
    List<Integer> replicaNodes,
    List<Integer> isrNodes,
    List<Integer> offlineReplicaNodes) {

  public PartitionDescription {
    replicaNodes = List.copyOf(replicaNodes);
    isrNodes = List.copyOf(isrNodes);
    offlineReplicaNodes = List.copyOf(offlineReplicaNodes);
  }
}
