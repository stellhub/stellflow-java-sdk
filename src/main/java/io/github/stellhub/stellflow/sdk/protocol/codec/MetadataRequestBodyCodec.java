package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataRequestBody;

/** Metadata 请求体编码器。 */
public class MetadataRequestBodyCodec implements RequestBodyCodec<MetadataRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.METADATA;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<MetadataRequestBody> bodyType() {
        return MetadataRequestBody.class;
    }

    @Override
    public void encode(MetadataRequestBody body, BinaryWriter writer) {
        writer.writeArray(body.topics(), topic -> writer.writeNullableString(topic.topic()));
        writer.writeBoolean(body.includeClusterAuthorizedOperations());
        writer.writeBoolean(body.includeTopicAuthorizedOperations());
        writer.writeBoolean(body.allowAutoTopicCreation());
    }
}
