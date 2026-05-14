package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.message.AbortedTransaction;
import io.github.stellhub.stellflow.sdk.protocol.message.FetchPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.FetchResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.FetchTopicResponse;

/** Fetch 响应体解码器。 */
public class FetchResponseBodyCodec implements ResponseBodyCodec<FetchResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.FETCH;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public FetchResponseBody decode(BinaryReader reader) {
        int sessionId = reader.readInt();
        return new FetchResponseBody(
                sessionId,
                reader.readArray(
                        () ->
                                new FetchTopicResponse(
                                        reader.readNullableString(),
                                        reader.readArray(
                                                () ->
                                                        new FetchPartitionResponse(
                                                                reader.readInt(),
                                                                ErrorCode.fromCode(reader.readShort()),
                                                                reader.readLong(),
                                                                reader.readLong(),
                                                                reader.readLong(),
                                                                reader.readArray(
                                                                        () ->
                                                                                new AbortedTransaction(
                                                                                        reader.readLong(), reader.readLong())),
                                                                reader.readBytes())))));
    }
}
