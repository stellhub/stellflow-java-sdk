package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;

/** JoinGroup 响应体。 */
public record JoinGroupResponseBody(
        ErrorCode errorCode, int generationId, String memberId, String leaderId)
        implements ResponseBody {}
