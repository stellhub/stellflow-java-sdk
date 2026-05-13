package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ProtocolEncodingException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;

/** 大端序二进制写入器。 */
public final class BinaryWriter {

  private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
  private final DataOutputStream output = new DataOutputStream(bytes);

  /** 写入 int8。 */
  public void writeByte(byte value) {
    writeIo(() -> output.writeByte(value));
  }

  /** 写入 int16。 */
  public void writeShort(short value) {
    writeIo(() -> output.writeShort(value));
  }

  /** 写入 int32。 */
  public void writeInt(int value) {
    writeIo(() -> output.writeInt(value));
  }

  /** 写入 int64。 */
  public void writeLong(long value) {
    writeIo(() -> output.writeLong(value));
  }

  /** 写入 bool。 */
  public void writeBoolean(boolean value) {
    writeByte((byte) (value ? 1 : 0));
  }

  /** 写入非空字符串。 */
  public void writeString(String value) {
    if (value == null) {
      throw new ProtocolEncodingException("string value must not be null");
    }
    writeNullableString(value);
  }

  /** 写入可空字符串。 */
  public void writeNullableString(String value) {
    if (value == null) {
      writeShort((short) -1);
      return;
    }
    byte[] data = value.getBytes(StandardCharsets.UTF_8);
    if (data.length > Short.MAX_VALUE) {
      throw new ProtocolEncodingException("string length exceeds int16 max: " + data.length);
    }
    writeShort((short) data.length);
    writeRawBytes(data);
  }

  /** 写入可空字节数组。 */
  public void writeBytes(byte[] value) {
    if (value == null) {
      writeInt(-1);
      return;
    }
    writeInt(value.length);
    writeRawBytes(value);
  }

  /** 写入不带长度前缀的原始字节。 */
  public void writeBytesWithoutLength(byte[] value) {
    writeRawBytes(value);
  }

  /** 写入字符串数组。 */
  public void writeStringArray(List<String> values) {
    writeArray(values, this::writeNullableString);
  }

  /** 写入 int32 数组。 */
  public void writeIntArray(List<Integer> values) {
    writeArray(values, this::writeInt);
  }

  /** 写入 int64 数组。 */
  public void writeLongArray(List<Long> values) {
    writeArray(values, this::writeLong);
  }

  /** 写入数组。 */
  public <T> void writeArray(List<T> values, Consumer<T> itemWriter) {
    if (values == null) {
      throw new ProtocolEncodingException("array value must not be null");
    }
    writeInt(values.size());
    for (T value : values) {
      itemWriter.accept(value);
    }
  }

  /** 返回已写入字节。 */
  public byte[] toByteArray() {
    writeIo(output::flush);
    return bytes.toByteArray();
  }

  private void writeRawBytes(byte[] value) {
    writeIo(() -> output.write(value));
  }

  private void writeIo(IoAction action) {
    try {
      action.run();
    } catch (IOException exception) {
      throw new ProtocolEncodingException("failed to write protocol bytes", exception);
    }
  }

  @FunctionalInterface
  private interface IoAction {
    void run() throws IOException;
  }
}
