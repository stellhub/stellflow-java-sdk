package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.message.ProducePartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.ProduceResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.ProduceTopicResponse;

/** Produce 响应体解码器。 */
public class ProduceResponseBodyCodec implements ResponseBodyCodec<ProduceResponseBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.PRODUCE;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public ProduceResponseBody decode(BinaryReader reader) {
    return new ProduceResponseBody(
        reader.readArray(
            () ->
                new ProduceTopicResponse(
                    reader.readNullableString(),
                    reader.readArray(
                        () ->
                            new ProducePartitionResponse(
                                reader.readInt(),
                                ErrorCode.fromCode(reader.readShort()),
                                reader.readLong(),
                                reader.readInt(),
                                reader.readLong(),
                                reader.readLong())))));
  }
}
