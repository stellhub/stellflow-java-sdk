package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.message.FindCoordinatorResponseBody;

/** FindCoordinator 响应体解码器。 */
public class FindCoordinatorResponseBodyCodec
        implements ResponseBodyCodec<FindCoordinatorResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.FIND_COORDINATOR;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public FindCoordinatorResponseBody decode(BinaryReader reader) {
        return new FindCoordinatorResponseBody(
                ErrorCode.fromCode(reader.readShort()),
                reader.readInt(),
                reader.readNullableString(),
                reader.readInt());
    }
}
