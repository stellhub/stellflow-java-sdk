package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.message.TopicAdminPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.TopicAdminResponseBody;

/** Topic 管理响应体解码器。 */
public class TopicAdminResponseBodyCodec implements ResponseBodyCodec<TopicAdminResponseBody> {

    private final ApiKey apiKey;

    public TopicAdminResponseBodyCodec(ApiKey apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public ApiKey apiKey() {
        return apiKey;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public TopicAdminResponseBody decode(BinaryReader reader) {
        return new TopicAdminResponseBody(
                reader.readNullableString(),
                reader.readArray(
                        () ->
                                new TopicAdminPartitionResponse(
                                        reader.readInt(), ErrorCode.fromCode(reader.readShort()), reader.readInt())));
    }
}
