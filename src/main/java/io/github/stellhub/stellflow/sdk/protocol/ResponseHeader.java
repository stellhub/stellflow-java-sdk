package io.github.stellhub.stellflow.sdk.protocol;

/** 统一响应头。 */
public record ResponseHeader(
        int correlationId, short headerVersion, ErrorCode errorCode, int throttleTimeMs) {}
