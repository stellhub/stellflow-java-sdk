package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.FetchRequestBody;

/** Fetch 请求体编码器。 */
public class FetchRequestBodyCodec implements RequestBodyCodec<FetchRequestBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.FETCH;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public Class<FetchRequestBody> bodyType() {
    return FetchRequestBody.class;
  }

  @Override
  public void encode(FetchRequestBody body, BinaryWriter writer) {
    writer.writeInt(body.replicaId());
    writer.writeInt(body.maxWaitMs());
    writer.writeInt(body.minBytes());
    writer.writeInt(body.maxBytes());
    writer.writeByte(body.isolationLevel());
    writer.writeInt(body.sessionId());
    writer.writeArray(
        body.topicPartitions(),
        topic -> {
          writer.writeNullableString(topic.topic());
          writer.writeArray(
              topic.partitions(),
              partition -> {
                writer.writeInt(partition.partition());
                writer.writeInt(partition.currentLeaderEpoch());
                writer.writeLong(partition.fetchOffset());
                writer.writeLong(partition.logStartOffset());
                writer.writeInt(partition.partitionMaxBytes());
              });
        });
  }
}
