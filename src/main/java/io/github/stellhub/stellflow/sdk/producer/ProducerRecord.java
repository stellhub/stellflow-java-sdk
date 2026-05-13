package io.github.stellhub.stellflow.sdk.producer;

/** Producer 待发送消息。 */
public record ProducerRecord(String topic, int partition, byte[] key, byte[] value) {

  public ProducerRecord {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("topic must not be blank");
    }
    if (partition < 0) {
      throw new IllegalArgumentException("partition must not be negative: " + partition);
    }
  }
}
