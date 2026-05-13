package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.ListOffsetsRequestBody;

/** ListOffsets 请求体编码器。 */
public class ListOffsetsRequestBodyCodec implements RequestBodyCodec<ListOffsetsRequestBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.LIST_OFFSETS;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public Class<ListOffsetsRequestBody> bodyType() {
    return ListOffsetsRequestBody.class;
  }

  @Override
  public void encode(ListOffsetsRequestBody body, BinaryWriter writer) {
    writer.writeInt(body.replicaId());
    writer.writeByte(body.isolationLevel());
    writer.writeArray(
        body.topics(),
        topic -> {
          writer.writeNullableString(topic.topic());
          writer.writeArray(
              topic.partitions(),
              partition -> {
                writer.writeInt(partition.partition());
                writer.writeInt(partition.currentLeaderEpoch());
                writer.writeLong(partition.timestamp());
                writer.writeInt(partition.maxNumOffsets());
              });
        });
  }
}
