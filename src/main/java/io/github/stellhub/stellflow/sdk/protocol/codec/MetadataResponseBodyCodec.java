package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataBroker;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataTopicResponse;
import java.util.List;

/** Metadata 响应体解码器。 */
public class MetadataResponseBodyCodec implements ResponseBodyCodec<MetadataResponseBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.METADATA;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public MetadataResponseBody decode(BinaryReader reader) {
    String clusterId = reader.readNullableString();
    int controllerId = reader.readInt();
    List<MetadataBroker> brokers =
        reader.readArray(
            () ->
                new MetadataBroker(
                    reader.readInt(),
                    reader.readNullableString(),
                    reader.readInt(),
                    reader.readNullableString()));
    List<MetadataTopicResponse> topics =
        reader.readArray(
            () -> {
              ErrorCode topicError = ErrorCode.fromCode(reader.readShort());
              String topic = reader.readNullableString();
              boolean internal = reader.readBoolean();
              List<MetadataPartitionResponse> partitions =
                  reader.readArray(
                      () ->
                          new MetadataPartitionResponse(
                              ErrorCode.fromCode(reader.readShort()),
                              reader.readInt(),
                              reader.readInt(),
                              reader.readInt(),
                              reader.readIntArray(),
                              reader.readIntArray(),
                              reader.readIntArray()));
              return new MetadataTopicResponse(
                  topicError, topic, internal, partitions, reader.readInt());
            });
    return new MetadataResponseBody(clusterId, controllerId, brokers, topics, reader.readInt());
  }
}
