package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetCommitPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetCommitResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetCommitTopicResponse;

/** OffsetCommit 响应体解码器。 */
public class OffsetCommitResponseBodyCodec implements ResponseBodyCodec<OffsetCommitResponseBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.OFFSET_COMMIT;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public OffsetCommitResponseBody decode(BinaryReader reader) {
    return new OffsetCommitResponseBody(
        reader.readArray(
            () ->
                new OffsetCommitTopicResponse(
                    reader.readNullableString(),
                    reader.readArray(
                        () ->
                            new OffsetCommitPartitionResponse(
                                reader.readInt(), ErrorCode.fromCode(reader.readShort()))))));
  }
}
