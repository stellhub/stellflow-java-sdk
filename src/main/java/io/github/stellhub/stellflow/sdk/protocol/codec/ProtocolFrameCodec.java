package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.EmptyResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.ProtocolDecodingException;
import io.github.stellhub.stellflow.sdk.protocol.RequestMessage;
import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.ResponseHeader;
import io.github.stellhub.stellflow.sdk.protocol.ResponseMessage;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** 协议帧编解码入口。 */
public class ProtocolFrameCodec {

  private final ProtocolCodecRegistry registry;

  public ProtocolFrameCodec(ProtocolCodecRegistry registry) {
    this.registry = registry;
  }

  /** 创建使用默认注册表的帧编解码器。 */
  public static ProtocolFrameCodec createDefault() {
    return new ProtocolFrameCodec(ProtocolCodecRegistry.defaultRegistry());
  }

  /** 编码完整请求帧。 */
  public byte[] encodeRequest(RequestMessage message) {
    BinaryWriter payload = new BinaryWriter();
    HeaderCodec.encodeRequestHeader(payload, message.header());
    registry.encodeRequestBody(
        message.header().apiKey(), message.header().apiVersion(), message.body(), payload);
    byte[] payloadBytes = payload.toByteArray();

    BinaryWriter frame = new BinaryWriter();
    frame.writeInt(payloadBytes.length);
    frame.writeBytesWithoutLength(payloadBytes);
    return frame.toByteArray();
  }

  /** 解码完整响应帧。 */
  public ResponseMessage decodeResponse(byte[] frameBytes, ApiKey apiKey, short apiVersion) {
    if (frameBytes.length < Integer.BYTES) {
      throw new ProtocolDecodingException("response frame is shorter than length prefix");
    }
    ByteBuffer buffer = ByteBuffer.wrap(frameBytes).order(ByteOrder.BIG_ENDIAN);
    int frameLength = buffer.getInt();
    if (frameLength != buffer.remaining()) {
      throw new ProtocolDecodingException(
          "invalid frame length, declared=%s, actual=%s"
              .formatted(frameLength, buffer.remaining()));
    }
    byte[] payload = new byte[buffer.remaining()];
    buffer.get(payload);

    return decodeResponsePayload(payload, apiKey, apiVersion);
  }

  /** 解码不含长度前缀的响应 payload。 */
  public ResponseMessage decodeResponsePayload(byte[] payload, ApiKey apiKey, short apiVersion) {
    BinaryReader reader = new BinaryReader(payload);
    ResponseHeader header = HeaderCodec.decodeResponseHeader(reader);
    ResponseBody body =
        reader.remaining() == 0
            ? EmptyResponseBody.INSTANCE
            : registry.decodeResponseBody(apiKey, apiVersion, reader);
    return new ResponseMessage(header, body);
  }
}
