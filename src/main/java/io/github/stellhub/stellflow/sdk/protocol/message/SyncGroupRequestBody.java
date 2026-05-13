package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.RequestBody;

/** SyncGroup 请求体。 */
public record SyncGroupRequestBody(String groupId, int generationId, String memberId)
    implements RequestBody {}
