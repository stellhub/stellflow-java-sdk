package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.SyncGroupRequestBody;

/** SyncGroup 请求体编码器。 */
public class SyncGroupRequestBodyCodec implements RequestBodyCodec<SyncGroupRequestBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.SYNC_GROUP;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public Class<SyncGroupRequestBody> bodyType() {
    return SyncGroupRequestBody.class;
  }

  @Override
  public void encode(SyncGroupRequestBody body, BinaryWriter writer) {
    writer.writeNullableString(body.groupId());
    writer.writeInt(body.generationId());
    writer.writeNullableString(body.memberId());
  }
}
