package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ProtocolDecodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** 大端序二进制读取器。 */
public final class BinaryReader {

    private final ByteBuffer buffer;

    public BinaryReader(byte[] data) {
        this(ByteBuffer.wrap(data));
    }

    public BinaryReader(ByteBuffer buffer) {
        this.buffer = buffer.slice().order(ByteOrder.BIG_ENDIAN);
    }

    /** 读取 int8。 */
    public byte readByte() {
        requireRemaining(Byte.BYTES);
        return buffer.get();
    }

    /** 读取 int16。 */
    public short readShort() {
        requireRemaining(Short.BYTES);
        return buffer.getShort();
    }

    /** 读取 int32。 */
    public int readInt() {
        requireRemaining(Integer.BYTES);
        return buffer.getInt();
    }

    /** 读取 int64。 */
    public long readLong() {
        requireRemaining(Long.BYTES);
        return buffer.getLong();
    }

    /** 读取 bool。 */
    public boolean readBoolean() {
        byte value = readByte();
        if (value == 0) {
            return false;
        }
        if (value == 1) {
            return true;
        }
        throw new ProtocolDecodingException("invalid boolean value: " + value);
    }

    /** 读取非空字符串。 */
    public String readString() {
        String value = readNullableString();
        if (value == null) {
            throw new ProtocolDecodingException("string value must not be null");
        }
        return value;
    }

    /** 读取可空字符串。 */
    public String readNullableString() {
        short length = readShort();
        if (length < 0) {
            return null;
        }
        requireRemaining(length);
        byte[] data = new byte[length];
        buffer.get(data);
        return new String(data, StandardCharsets.UTF_8);
    }

    /** 读取可空字节数组。 */
    public byte[] readBytes() {
        int length = readInt();
        if (length < 0) {
            return null;
        }
        requireRemaining(length);
        byte[] data = new byte[length];
        buffer.get(data);
        return data;
    }

    /** 读取字符串数组。 */
    public List<String> readStringArray() {
        return readArray(this::readNullableString);
    }

    /** 读取 int32 数组。 */
    public List<Integer> readIntArray() {
        return readArray(this::readInt);
    }

    /** 读取 int64 数组。 */
    public List<Long> readLongArray() {
        return readArray(this::readLong);
    }

    /** 读取数组。 */
    public <T> List<T> readArray(Supplier<T> itemReader) {
        int length = readInt();
        if (length < 0) {
            throw new ProtocolDecodingException("array length must not be negative: " + length);
        }
        List<T> values = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            values.add(itemReader.get());
        }
        return values;
    }

    /** 返回剩余字节数。 */
    public int remaining() {
        return buffer.remaining();
    }

    private void requireRemaining(int length) {
        if (buffer.remaining() < length) {
            throw new ProtocolDecodingException(
                    "not enough protocol bytes, need=" + length + ", remaining=" + buffer.remaining());
        }
    }
}
