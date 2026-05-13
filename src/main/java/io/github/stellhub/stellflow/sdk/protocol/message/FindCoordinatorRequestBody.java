package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.RequestBody;

/** FindCoordinator 请求体。 */
public record FindCoordinatorRequestBody(String key, byte keyType) implements RequestBody {}
