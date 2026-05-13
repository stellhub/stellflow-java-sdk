package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;
import java.util.List;

/** Produce 响应体。 */
public record ProduceResponseBody(List<ProduceTopicResponse> responses) implements ResponseBody {}
