package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.RequestBody;
import java.util.List;

/** Metadata 请求体。 */
public record MetadataRequestBody(
        List<MetadataTopicRequest> topics,
        boolean includeClusterAuthorizedOperations,
        boolean includeTopicAuthorizedOperations,
        boolean allowAutoTopicCreation)
        implements RequestBody {}
