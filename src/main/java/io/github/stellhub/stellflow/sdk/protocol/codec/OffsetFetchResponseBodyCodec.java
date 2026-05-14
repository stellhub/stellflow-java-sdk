package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetFetchPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetFetchResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetFetchTopicResponse;

/** OffsetFetch 响应体解码器。 */
public class OffsetFetchResponseBodyCodec implements ResponseBodyCodec<OffsetFetchResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_FETCH;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public OffsetFetchResponseBody decode(BinaryReader reader) {
        return new OffsetFetchResponseBody(
                reader.readArray(
                        () ->
                                new OffsetFetchTopicResponse(
                                        reader.readNullableString(),
                                        reader.readArray(
                                                () ->
                                                        new OffsetFetchPartitionResponse(
                                                                reader.readInt(),
                                                                reader.readLong(),
                                                                reader.readNullableString(),
                                                                ErrorCode.fromCode(reader.readShort()))))));
    }
}
