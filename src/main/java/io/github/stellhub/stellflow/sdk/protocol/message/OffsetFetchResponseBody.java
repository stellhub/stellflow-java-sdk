package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;
import java.util.List;

/** OffsetFetch 响应体。 */
public record OffsetFetchResponseBody(List<OffsetFetchTopicResponse> topics)
    implements ResponseBody {}
