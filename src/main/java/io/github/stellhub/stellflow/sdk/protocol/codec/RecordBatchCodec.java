package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ProtocolDecodingException;
import io.github.stellhub.stellflow.sdk.protocol.message.RecordBatch;
import io.github.stellhub.stellflow.sdk.protocol.message.StellflowRecord;
import io.github.stellhub.stellflow.sdk.protocol.message.StellflowRecordHeader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/** RecordBatch 编解码器。 */
public final class RecordBatchCodec {

  private static final int BATCH_PREFIX_BYTES = Integer.BYTES + Integer.BYTES;
  private static final int CRC_PAYLOAD_OFFSET =
      Integer.BYTES + Integer.BYTES + Integer.BYTES + Byte.BYTES + Integer.BYTES;

  private RecordBatchCodec() {}

  /** 编码单个 RecordBatch。 */
  public static byte[] encode(RecordBatch batch) {
    BinaryWriter crcPayload = new BinaryWriter();
    crcPayload.writeShort(batch.attributes());
    crcPayload.writeInt(batch.lastOffsetDelta());
    crcPayload.writeLong(batch.baseTimestamp());
    crcPayload.writeLong(batch.maxTimestamp());
    crcPayload.writeLong(batch.producerId());
    crcPayload.writeShort(batch.producerEpoch());
    crcPayload.writeInt(batch.baseSequence());
    crcPayload.writeInt(batch.records().size());
    for (StellflowRecord record : batch.records()) {
      writeRecord(crcPayload, record);
    }
    byte[] crcPayloadBytes = crcPayload.toByteArray();
    int crc32c = crc32c(crcPayloadBytes, 0, crcPayloadBytes.length);
    int batchLength = Integer.BYTES + Byte.BYTES + Integer.BYTES + crcPayloadBytes.length;

    BinaryWriter writer = new BinaryWriter();
    writer.writeInt(batch.baseOffsetDelta());
    writer.writeInt(batchLength);
    writer.writeInt(batch.partitionLeaderEpoch());
    writer.writeByte(batch.magic());
    writer.writeInt(crc32c);
    writer.writeBytesWithoutLength(crcPayloadBytes);
    return writer.toByteArray();
  }

  /** 解码单个 RecordBatch。 */
  public static RecordBatch decode(byte[] batchBytes) {
    if (batchBytes.length < CRC_PAYLOAD_OFFSET) {
      throw new ProtocolDecodingException("record batch is too short: " + batchBytes.length);
    }
    BinaryReader reader = new BinaryReader(batchBytes);
    int baseOffsetDelta = reader.readInt();
    int batchLength = reader.readInt();
    if (batchLength != batchBytes.length - BATCH_PREFIX_BYTES) {
      throw new ProtocolDecodingException(
          "invalid record batch length, declared=%s, actual=%s"
              .formatted(batchLength, batchBytes.length - BATCH_PREFIX_BYTES));
    }
    int partitionLeaderEpoch = reader.readInt();
    byte magic = reader.readByte();
    int expectedCrc32c = reader.readInt();
    int actualCrc32c =
        crc32c(batchBytes, CRC_PAYLOAD_OFFSET, batchBytes.length - CRC_PAYLOAD_OFFSET);
    if (expectedCrc32c != actualCrc32c) {
      throw new ProtocolDecodingException(
          "invalid record batch crc32c, expected=%s, actual=%s"
              .formatted(expectedCrc32c, actualCrc32c));
    }

    short attributes = reader.readShort();
    int lastOffsetDelta = reader.readInt();
    long baseTimestamp = reader.readLong();
    long maxTimestamp = reader.readLong();
    long producerId = reader.readLong();
    short producerEpoch = reader.readShort();
    int baseSequence = reader.readInt();
    int recordCount = reader.readInt();
    if (recordCount < 0) {
      throw new ProtocolDecodingException("record count must not be negative: " + recordCount);
    }
    List<StellflowRecord> records = new ArrayList<>(recordCount);
    for (int index = 0; index < recordCount; index++) {
      records.add(readRecord(reader));
    }
    return new RecordBatch(
        baseOffsetDelta,
        batchLength,
        partitionLeaderEpoch,
        magic,
        expectedCrc32c,
        attributes,
        lastOffsetDelta,
        baseTimestamp,
        maxTimestamp,
        producerId,
        producerEpoch,
        baseSequence,
        records);
  }

  /** 编码 RecordBatchSet。 */
  public static byte[] encodeBatchSet(List<RecordBatch> batches) {
    BinaryWriter writer = new BinaryWriter();
    for (RecordBatch batch : batches) {
      writer.writeBytesWithoutLength(encode(batch));
    }
    return writer.toByteArray();
  }

  /** 解码 RecordBatchSet。 */
  public static List<RecordBatch> decodeBatchSet(byte[] batchSetBytes) {
    ByteBuffer buffer = ByteBuffer.wrap(batchSetBytes).order(ByteOrder.BIG_ENDIAN);
    List<RecordBatch> batches = new ArrayList<>();
    while (buffer.hasRemaining()) {
      if (buffer.remaining() < BATCH_PREFIX_BYTES) {
        throw new ProtocolDecodingException("truncated record batch set");
      }
      int start = buffer.position();
      buffer.getInt();
      int batchLength = buffer.getInt();
      int totalLength = BATCH_PREFIX_BYTES + batchLength;
      if (batchLength < 0 || buffer.limit() - start < totalLength) {
        throw new ProtocolDecodingException(
            "invalid record batch length in batch set: " + batchLength);
      }
      byte[] batchBytes = new byte[totalLength];
      buffer.position(start);
      buffer.get(batchBytes);
      batches.add(decode(batchBytes));
    }
    return batches;
  }

  private static void writeRecord(BinaryWriter writer, StellflowRecord record) {
    BinaryWriter payload = new BinaryWriter();
    payload.writeByte(record.attributes());
    payload.writeLong(record.timestampDelta());
    payload.writeInt(record.offsetDelta());
    payload.writeBytes(record.key());
    payload.writeBytes(record.value());
    payload.writeArray(
        record.headers(),
        header -> {
          payload.writeNullableString(header.key());
          payload.writeBytes(header.value());
        });
    byte[] payloadBytes = payload.toByteArray();
    writer.writeInt(payloadBytes.length);
    writer.writeBytesWithoutLength(payloadBytes);
  }

  private static StellflowRecord readRecord(BinaryReader reader) {
    int length = reader.readInt();
    if (length < 0 || reader.remaining() < length) {
      throw new ProtocolDecodingException("invalid record length: " + length);
    }
    byte[] payloadBytes = new byte[length];
    for (int index = 0; index < length; index++) {
      payloadBytes[index] = reader.readByte();
    }
    BinaryReader payload = new BinaryReader(payloadBytes);
    StellflowRecord record =
        new StellflowRecord(
            payload.readByte(),
            payload.readLong(),
            payload.readInt(),
            payload.readBytes(),
            payload.readBytes(),
            payload.readArray(
                () ->
                    new StellflowRecordHeader(payload.readNullableString(), payload.readBytes())));
    if (payload.remaining() != 0) {
      throw new ProtocolDecodingException("record has trailing bytes: " + payload.remaining());
    }
    return record;
  }

  private static int crc32c(byte[] data, int offset, int length) {
    CRC32C crc32c = new CRC32C();
    crc32c.update(data, offset, length);
    return (int) crc32c.getValue();
  }
}
