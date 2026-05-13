package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.message.HeartbeatResponseBody;

/** Heartbeat 响应体解码器。 */
public class HeartbeatResponseBodyCodec implements ResponseBodyCodec<HeartbeatResponseBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.HEARTBEAT;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public HeartbeatResponseBody decode(BinaryReader reader) {
    return new HeartbeatResponseBody(ErrorCode.fromCode(reader.readShort()));
  }
}
