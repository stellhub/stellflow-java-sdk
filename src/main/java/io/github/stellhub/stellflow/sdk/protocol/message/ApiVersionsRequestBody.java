package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.RequestBody;
import java.util.List;

/** ApiVersions 请求体。 */
public record ApiVersionsRequestBody(
        String clientSoftwareName, String clientSoftwareVersion, List<String> supportedFeatures)
        implements RequestBody {}
