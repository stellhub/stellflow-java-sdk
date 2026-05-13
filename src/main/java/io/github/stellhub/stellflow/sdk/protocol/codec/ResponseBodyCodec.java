package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;

/** 响应体解码器。 */
public interface ResponseBodyCodec<T extends ResponseBody> {

  /** 返回 API 标识。 */
  ApiKey apiKey();

  /** 返回 API 版本。 */
  short apiVersion();

  /** 解码响应体。 */
  T decode(BinaryReader reader);
}
