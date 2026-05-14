package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.message.ListOffsetsPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.ListOffsetsResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.ListOffsetsTopicResponse;

/** ListOffsets 响应体解码器。 */
public class ListOffsetsResponseBodyCodec implements ResponseBodyCodec<ListOffsetsResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.LIST_OFFSETS;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public ListOffsetsResponseBody decode(BinaryReader reader) {
        return new ListOffsetsResponseBody(
                reader.readArray(
                        () ->
                                new ListOffsetsTopicResponse(
                                        reader.readNullableString(),
                                        reader.readArray(
                                                () ->
                                                        new ListOffsetsPartitionResponse(
                                                                reader.readInt(),
                                                                ErrorCode.fromCode(reader.readShort()),
                                                                reader.readInt(),
                                                                reader.readLong(),
                                                                reader.readLong(),
                                                                reader.readLongArray())))));
    }
}
