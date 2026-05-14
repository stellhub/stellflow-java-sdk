package io.github.stellhub.stellflow.sdk.protocol;

/** 统一请求头。 */
public record RequestHeader(
        short apiKeyCode,
        short apiVersion,
        short headerVersion,
        int correlationId,
        String clientId,
        String traceId,
        String spanId,
        byte traceFlags,
        String tenantId,
        String quotaKey,
        String authContextId,
        byte trafficClass,
        String trafficTag,
        short flags) {

    /** 创建默认请求头。 */
    public static RequestHeader of(
            ApiKey apiKey, short apiVersion, int correlationId, String clientId) {
        return new RequestHeader(
                apiKey.code(),
                apiVersion,
                ProtocolConstants.HEADER_VERSION,
                correlationId,
                clientId,
                null,
                null,
                (byte) 0,
                null,
                null,
                null,
                (byte) 0,
                null,
                (short) 0);
    }

    /** 返回请求头对应的 API 标识。 */
    public ApiKey apiKey() {
        return ApiKey.fromCode(apiKeyCode);
    }
}
