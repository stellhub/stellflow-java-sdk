package io.github.stellhub.stellflow.sdk.protocol.message;

/** 消息记录头。 */
public record StellflowRecordHeader(String key, byte[] value) {}
