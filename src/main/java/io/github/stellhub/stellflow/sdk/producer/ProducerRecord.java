package io.github.stellhub.stellflow.sdk.producer;

/** Producer 待发送消息。 */
public record ProducerRecord(String topic, int partition, byte[] key, byte[] value) {

    public static final int NO_PARTITION = -1;

    public ProducerRecord(String topic, byte[] key, byte[] value) {
        this(topic, NO_PARTITION, key, value);
    }

    public ProducerRecord {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (partition < NO_PARTITION) {
            throw new IllegalArgumentException("partition must be >= -1: " + partition);
        }
    }
}
