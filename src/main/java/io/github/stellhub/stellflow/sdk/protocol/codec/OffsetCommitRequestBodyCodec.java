package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetCommitRequestBody;

/** OffsetCommit 请求体编码器。 */
public class OffsetCommitRequestBodyCodec implements RequestBodyCodec<OffsetCommitRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_COMMIT;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<OffsetCommitRequestBody> bodyType() {
        return OffsetCommitRequestBody.class;
    }

    @Override
    public void encode(OffsetCommitRequestBody body, BinaryWriter writer) {
        writer.writeNullableString(body.groupId());
        writer.writeArray(
                body.topics(),
                topic -> {
                    writer.writeNullableString(topic.topic());
                    writer.writeArray(
                            topic.partitions(),
                            partition -> {
                                writer.writeInt(partition.partition());
                                writer.writeLong(partition.offset());
                                writer.writeNullableString(partition.metadata());
                            });
                });
    }
}
