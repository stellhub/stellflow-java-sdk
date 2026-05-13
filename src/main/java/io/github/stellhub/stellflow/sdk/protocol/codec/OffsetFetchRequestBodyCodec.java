package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetFetchRequestBody;

/** OffsetFetch 请求体编码器。 */
public class OffsetFetchRequestBodyCodec implements RequestBodyCodec<OffsetFetchRequestBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.OFFSET_FETCH;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public Class<OffsetFetchRequestBody> bodyType() {
    return OffsetFetchRequestBody.class;
  }

  @Override
  public void encode(OffsetFetchRequestBody body, BinaryWriter writer) {
    writer.writeNullableString(body.groupId());
    writer.writeArray(
        body.topics(),
        topic -> {
          writer.writeNullableString(topic.topic());
          writer.writeArray(
              topic.partitions(), partition -> writer.writeInt(partition.partition()));
        });
  }
}
