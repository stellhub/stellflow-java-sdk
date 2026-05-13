package io.github.stellhub.stellflow.sdk.producer;

/** Produce 成功后的消息元数据。 */
public record RecordMetadata(
    String topic, int partition, long baseOffset, int leaderEpoch, long logStartOffset) {}
