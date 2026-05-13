package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;

/** SyncGroup 响应体。 */
public record SyncGroupResponseBody(ErrorCode errorCode) implements ResponseBody {}
