package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.RequestBody;
import java.util.List;

/** Produce 请求体。 */
public record ProduceRequestBody(
        String transactionalId, short acks, int timeoutMs, List<ProduceTopicData> topicData)
        implements RequestBody {}
