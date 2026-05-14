package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.message.SyncGroupResponseBody;

/** SyncGroup 响应体解码器。 */
public class SyncGroupResponseBodyCodec implements ResponseBodyCodec<SyncGroupResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.SYNC_GROUP;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public SyncGroupResponseBody decode(BinaryReader reader) {
        return new SyncGroupResponseBody(ErrorCode.fromCode(reader.readShort()));
    }
}
