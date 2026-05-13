package io.github.stellhub.stellflow.sdk.metadata;

import io.github.stellhub.stellflow.sdk.network.BrokerEndpoint;
import java.util.List;

/** 分区路由信息。 */
public record PartitionRoute(
    String topic,
    int partition,
    int leaderId,
    int leaderEpoch,
    BrokerEndpoint leaderEndpoint,
    List<Integer> replicaNodes,
    List<Integer> isrNodes) {}
