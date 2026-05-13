package io.github.stellhub.stellflow.sdk.protocol;

/** 完整请求消息。 */
public record RequestMessage(RequestHeader header, RequestBody body) {}
