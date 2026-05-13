package io.github.stellhub.stellflow.sdk.protocol;

/** 完整响应消息。 */
public record ResponseMessage(ResponseHeader header, ResponseBody body) {}
