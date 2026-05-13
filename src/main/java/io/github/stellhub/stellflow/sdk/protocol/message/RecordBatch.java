package io.github.stellhub.stellflow.sdk.protocol.message;

import java.util.List;

/** RecordBatch 消息批。 */
public record RecordBatch(
    int baseOffsetDelta,
    int batchLength,
    int partitionLeaderEpoch,
    byte magic,
    int crc32c,
    short attributes,
    int lastOffsetDelta,
    long baseTimestamp,
    long maxTimestamp,
    long producerId,
    short producerEpoch,
    int baseSequence,
    List<StellflowRecord> records) {

  public static final byte MAGIC_V1 = 1;

  /** 创建待编码的第一版消息批。 */
  public static RecordBatch create(
      int baseOffsetDelta,
      int partitionLeaderEpoch,
      short attributes,
      int lastOffsetDelta,
      long baseTimestamp,
      long maxTimestamp,
      long producerId,
      short producerEpoch,
      int baseSequence,
      List<StellflowRecord> records) {
    return new RecordBatch(
        baseOffsetDelta,
        0,
        partitionLeaderEpoch,
        MAGIC_V1,
        0,
        attributes,
        lastOffsetDelta,
        baseTimestamp,
        maxTimestamp,
        producerId,
        producerEpoch,
        baseSequence,
        records);
  }
}
