package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;

/** FindCoordinator 响应体。 */
public record FindCoordinatorResponseBody(ErrorCode errorCode, int nodeId, String host, int port)
        implements ResponseBody {}
