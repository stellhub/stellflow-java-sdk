package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;
import java.util.List;

/** ApiVersions 响应体。 */
public record ApiVersionsResponseBody(
    List<ApiVersionRange> apiVersions,
    String brokerSoftwareName,
    String brokerSoftwareVersion,
    List<String> supportedFeatures)
    implements ResponseBody {}
