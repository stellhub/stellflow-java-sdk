package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.FindCoordinatorRequestBody;

/** FindCoordinator 请求体编码器。 */
public class FindCoordinatorRequestBodyCodec
        implements RequestBodyCodec<FindCoordinatorRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.FIND_COORDINATOR;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<FindCoordinatorRequestBody> bodyType() {
        return FindCoordinatorRequestBody.class;
    }

    @Override
    public void encode(FindCoordinatorRequestBody body, BinaryWriter writer) {
        writer.writeNullableString(body.key());
        writer.writeByte(body.keyType());
    }
}
