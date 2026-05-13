package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.HeartbeatRequestBody;

/** Heartbeat 请求体编码器。 */
public class HeartbeatRequestBodyCodec implements RequestBodyCodec<HeartbeatRequestBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.HEARTBEAT;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public Class<HeartbeatRequestBody> bodyType() {
    return HeartbeatRequestBody.class;
  }

  @Override
  public void encode(HeartbeatRequestBody body, BinaryWriter writer) {
    writer.writeNullableString(body.groupId());
    writer.writeInt(body.generationId());
    writer.writeNullableString(body.memberId());
  }
}
