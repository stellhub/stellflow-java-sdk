package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.RequestBody;

/** 请求体编码器。 */
public interface RequestBodyCodec<T extends RequestBody> {

    /** 返回 API 标识。 */
    ApiKey apiKey();

    /** 返回 API 版本。 */
    short apiVersion();

    /** 返回请求体类型。 */
    Class<T> bodyType();

    /** 编码请求体。 */
    void encode(T body, BinaryWriter writer);
}
