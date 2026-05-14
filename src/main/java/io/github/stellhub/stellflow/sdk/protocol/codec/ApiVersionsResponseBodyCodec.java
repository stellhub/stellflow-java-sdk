package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.ApiVersionRange;
import io.github.stellhub.stellflow.sdk.protocol.message.ApiVersionsResponseBody;
import java.util.List;

/** ApiVersions 响应体解码器。 */
public class ApiVersionsResponseBodyCodec implements ResponseBodyCodec<ApiVersionsResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.API_VERSIONS;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public ApiVersionsResponseBody decode(BinaryReader reader) {
        List<ApiVersionRange> apiVersions =
                reader.readArray(
                        () -> new ApiVersionRange(reader.readShort(), reader.readShort(), reader.readShort()));
        return new ApiVersionsResponseBody(
                apiVersions,
                reader.readNullableString(),
                reader.readNullableString(),
                reader.readStringArray());
    }
}
