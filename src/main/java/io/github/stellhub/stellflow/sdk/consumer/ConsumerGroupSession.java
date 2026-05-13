package io.github.stellhub.stellflow.sdk.consumer;

/** 消费组会话信息。 */
public record ConsumerGroupSession(
    String groupId, int generationId, String memberId, String leaderId) {}
