package io.github.stellhub.stellflow.sdk.consumer;

/** Consumer 拉取到的消息。 */
public record ConsumerRecord(
    String topic, int partition, long offset, byte[] key, byte[] value, long timestamp) {}
