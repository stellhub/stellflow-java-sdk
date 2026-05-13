package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.ProduceRequestBody;

/** Produce 请求体编码器。 */
public class ProduceRequestBodyCodec implements RequestBodyCodec<ProduceRequestBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.PRODUCE;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public Class<ProduceRequestBody> bodyType() {
    return ProduceRequestBody.class;
  }

  @Override
  public void encode(ProduceRequestBody body, BinaryWriter writer) {
    writer.writeNullableString(body.transactionalId());
    writer.writeShort(body.acks());
    writer.writeInt(body.timeoutMs());
    writer.writeArray(
        body.topicData(),
        topic -> {
          writer.writeNullableString(topic.topic());
          writer.writeArray(
              topic.partitions(),
              partition -> {
                writer.writeInt(partition.partition());
                writer.writeBytes(partition.records());
              });
        });
  }
}
