package io.github.stellhub.stellflow.sdk.admin;

import io.github.stellhub.stellflow.sdk.protocol.message.MetadataBroker;
import java.util.List;

/** 集群描述结果。 */
public record ClusterDescription(
        String clusterId,
        int controllerId,
        List<MetadataBroker> brokers,
        int clusterAuthorizedOperations) {

    public ClusterDescription {
        brokers = List.copyOf(brokers);
    }
}
