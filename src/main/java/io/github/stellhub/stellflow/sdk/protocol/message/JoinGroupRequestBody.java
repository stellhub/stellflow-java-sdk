package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.RequestBody;

/** JoinGroup 请求体。 */
public record JoinGroupRequestBody(String groupId, String memberId, int sessionTimeoutMs)
        implements RequestBody {}
