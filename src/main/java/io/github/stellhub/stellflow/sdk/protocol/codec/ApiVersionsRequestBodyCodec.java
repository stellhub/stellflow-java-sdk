package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.message.ApiVersionsRequestBody;

/** ApiVersions 请求体编码器。 */
public class ApiVersionsRequestBodyCodec implements RequestBodyCodec<ApiVersionsRequestBody> {

  @Override
  public ApiKey apiKey() {
    return ApiKey.API_VERSIONS;
  }

  @Override
  public short apiVersion() {
    return 0;
  }

  @Override
  public Class<ApiVersionsRequestBody> bodyType() {
    return ApiVersionsRequestBody.class;
  }

  @Override
  public void encode(ApiVersionsRequestBody body, BinaryWriter writer) {
    writer.writeNullableString(body.clientSoftwareName());
    writer.writeNullableString(body.clientSoftwareVersion());
    writer.writeStringArray(body.supportedFeatures());
  }
}
