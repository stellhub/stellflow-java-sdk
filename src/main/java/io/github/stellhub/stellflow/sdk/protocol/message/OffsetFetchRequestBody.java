package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.RequestBody;
import java.util.List;

/** OffsetFetch 请求体。 */
public record OffsetFetchRequestBody(String groupId, List<OffsetFetchTopic> topics)
    implements RequestBody {}
