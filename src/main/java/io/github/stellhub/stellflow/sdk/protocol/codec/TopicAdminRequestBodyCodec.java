package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.TopicAdminRequestBody;

/** Topic 管理请求体编码器。 */
public class TopicAdminRequestBodyCodec implements RequestBodyCodec<TopicAdminRequestBody> {

    private final ApiKey apiKey;

    public TopicAdminRequestBodyCodec(ApiKey apiKey) {
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
    public Class<TopicAdminRequestBody> bodyType() {
        return TopicAdminRequestBody.class;
    }

    @Override
    public void encode(TopicAdminRequestBody body, BinaryWriter writer) {
        writer.writeNullableString(body.topic());
        writer.writeInt(body.partitionCount());
        writer.writeInt(body.partition());
        writer.writeInt(body.leaderId());
        writer.writeInt(body.leaderEpoch());
        writer.writeIntArray(body.replicaNodes());
        writer.writeIntArray(body.isrNodes());
    }
}
