package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;
import java.util.List;

/** ListOffsets 响应体。 */
public record ListOffsetsResponseBody(List<ListOffsetsTopicResponse> topics)
        implements ResponseBody {}
