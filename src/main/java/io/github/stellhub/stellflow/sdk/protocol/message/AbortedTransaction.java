package io.github.stellhub.stellflow.sdk.protocol.message;

/** 已中止事务。 */
public record AbortedTransaction(long producerId, long firstOffset) {}
