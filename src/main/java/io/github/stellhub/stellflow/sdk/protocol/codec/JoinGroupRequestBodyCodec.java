package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.JoinGroupRequestBody;

/** JoinGroup 请求体编码器。 */
public class JoinGroupRequestBodyCodec implements RequestBodyCodec<JoinGroupRequestBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.JOIN_GROUP;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public Class<JoinGroupRequestBody> bodyType() {
    return JoinGroupRequestBody.class;
  }

  @Override
  public void encode(JoinGroupRequestBody body, BinaryWriter writer) {
    writer.writeNullableString(body.groupId());
    writer.writeNullableString(body.memberId());
    writer.writeInt(body.sessionTimeoutMs());
  }
}
