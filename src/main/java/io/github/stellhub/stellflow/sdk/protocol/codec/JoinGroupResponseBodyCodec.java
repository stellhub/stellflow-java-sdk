package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.message.JoinGroupResponseBody;

/** JoinGroup 响应体解码器。 */
public class JoinGroupResponseBodyCodec implements ResponseBodyCodec<JoinGroupResponseBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.JOIN_GROUP;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public JoinGroupResponseBody decode(BinaryReader reader) {
    return new JoinGroupResponseBody(
        ErrorCode.fromCode(reader.readShort()),
        reader.readInt(),
        reader.readNullableString(),
        reader.readNullableString());
  }
}
