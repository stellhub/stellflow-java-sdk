package io.github.stellhub.stellflow.sdk.consumer;

import io.github.stellhub.stellflow.sdk.metadata.TopicPartition;
import io.github.stellhub.stellflow.sdk.protocol.codec.BinaryReader;
import io.github.stellhub.stellflow.sdk.protocol.codec.BinaryWriter;
import java.util.List;

/** Consumer assignment payload 编解码器。 */
public final class ConsumerAssignmentPayloadCodec {

  private ConsumerAssignmentPayloadCodec() {}

  /** 编码订阅 payload。 */
  public static byte[] encodeSubscription(ConsumerSubscriptionPayload payload) {
    BinaryWriter writer = new BinaryWriter();
    writer.writeNullableString(payload.memberId());
    writer.writeStringArray(payload.topics());
    return writer.toByteArray();
  }

  /** 解码订阅 payload。 */
  public static ConsumerSubscriptionPayload decodeSubscription(byte[] bytes) {
    BinaryReader reader = new BinaryReader(bytes);
    return new ConsumerSubscriptionPayload(reader.readNullableString(), reader.readStringArray());
  }

  /** 编码分配 payload。 */
  public static byte[] encodeAssignment(ConsumerAssignmentPayload payload) {
    BinaryWriter writer = new BinaryWriter();
    writer.writeArray(
        payload.partitions(),
        partition -> {
          writer.writeNullableString(partition.topic());
          writer.writeInt(partition.partition());
        });
    return writer.toByteArray();
  }

  /** 解码分配 payload。 */
  public static ConsumerAssignmentPayload decodeAssignment(byte[] bytes) {
    BinaryReader reader = new BinaryReader(bytes);
    List<TopicPartition> partitions =
        reader.readArray(() -> new TopicPartition(reader.readNullableString(), reader.readInt()));
    return new ConsumerAssignmentPayload(partitions);
  }
}
