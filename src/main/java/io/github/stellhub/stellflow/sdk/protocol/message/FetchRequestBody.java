package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.RequestBody;
import java.util.List;

/** Fetch 请求体。 */
public record FetchRequestBody(
        int replicaId,
        int maxWaitMs,
        int minBytes,
        int maxBytes,
        byte isolationLevel,
        int sessionId,
        List<FetchTopicRequest> topicPartitions)
        implements RequestBody {}
