package io.github.stellhub.stellflow.sdk.consumer;

/** 已提交 offset 信息。 */
public record OffsetAndMetadata(long offset, String metadata) {}
