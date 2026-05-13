package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;

/** Heartbeat 响应体。 */
public record HeartbeatResponseBody(ErrorCode errorCode) implements ResponseBody {}
