package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.RequestBody;

/** Heartbeat 请求体。 */
public record HeartbeatRequestBody(String groupId, int generationId, String memberId)
    implements RequestBody {}
