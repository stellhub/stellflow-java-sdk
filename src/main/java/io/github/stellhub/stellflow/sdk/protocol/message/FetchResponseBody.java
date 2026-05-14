package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;
import java.util.List;

/** Fetch 响应体。 */
public record FetchResponseBody(int sessionId, List<FetchTopicResponse> responses)
        implements ResponseBody {}
