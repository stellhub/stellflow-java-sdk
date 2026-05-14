package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;
import java.util.List;

/** Metadata 响应体。 */
public record MetadataResponseBody(
        String clusterId,
        int controllerId,
        List<MetadataBroker> brokers,
        List<MetadataTopicResponse> topics,
        int clusterAuthorizedOperations)
        implements ResponseBody {}
