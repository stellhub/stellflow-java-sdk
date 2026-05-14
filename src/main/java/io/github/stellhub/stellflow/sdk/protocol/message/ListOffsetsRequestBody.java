package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.RequestBody;
import java.util.List;

/** ListOffsets 请求体。 */
public record ListOffsetsRequestBody(
        int replicaId, byte isolationLevel, List<ListOffsetsTopicRequest> topics)
        implements RequestBody {}
