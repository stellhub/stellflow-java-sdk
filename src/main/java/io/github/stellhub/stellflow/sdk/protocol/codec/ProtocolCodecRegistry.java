package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ProtocolEncodingException;
import io.github.stellhub.stellflow.sdk.protocol.ProtocolException;
import io.github.stellhub.stellflow.sdk.protocol.RequestBody;
import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;
import java.util.HashMap;
import java.util.Map;

/** 协议编解码注册表。 */
public class ProtocolCodecRegistry {

  private final Map<RequestCodecKey, RequestBodyCodec<? extends RequestBody>> requestCodecs =
      new HashMap<>();
  private final Map<ResponseCodecKey, ResponseBodyCodec<? extends ResponseBody>> responseCodecs =
      new HashMap<>();

  /** 注册请求体编码器。 */
  public void registerRequestCodec(RequestBodyCodec<? extends RequestBody> codec) {
    requestCodecs.put(
        new RequestCodecKey(codec.apiKey(), codec.apiVersion(), codec.bodyType()), codec);
  }

  /** 注册响应体解码器。 */
  public void registerResponseCodec(ResponseBodyCodec<? extends ResponseBody> codec) {
    responseCodecs.put(new ResponseCodecKey(codec.apiKey(), codec.apiVersion()), codec);
  }

  /** 编码请求体。 */
  @SuppressWarnings("unchecked")
  public void encodeRequestBody(
      ApiKey apiKey, short apiVersion, RequestBody body, BinaryWriter writer) {
    RequestBodyCodec<RequestBody> codec =
        (RequestBodyCodec<RequestBody>)
            requestCodecs.get(new RequestCodecKey(apiKey, apiVersion, body.getClass()));
    if (codec == null) {
      throw new ProtocolEncodingException(
          "No request codec for apiKey=%s, apiVersion=%s, bodyType=%s"
              .formatted(apiKey, apiVersion, body.getClass().getName()));
    }
    codec.encode(body, writer);
  }

  /** 解码响应体。 */
  public ResponseBody decodeResponseBody(ApiKey apiKey, short apiVersion, BinaryReader reader) {
    ResponseBodyCodec<? extends ResponseBody> codec =
        responseCodecs.get(new ResponseCodecKey(apiKey, apiVersion));
    if (codec == null) {
      throw new ProtocolException(
          "No response codec for apiKey=%s, apiVersion=%s".formatted(apiKey, apiVersion));
    }
    return codec.decode(reader);
  }

  /** 创建默认注册表。 */
  public static ProtocolCodecRegistry defaultRegistry() {
    ProtocolCodecRegistry registry = new ProtocolCodecRegistry();
    registry.registerRequestCodec(new ApiVersionsRequestBodyCodec());
    registry.registerRequestCodec(new MetadataRequestBodyCodec());
    registry.registerRequestCodec(new ProduceRequestBodyCodec());
    registry.registerRequestCodec(new FetchRequestBodyCodec());
    registry.registerRequestCodec(new ListOffsetsRequestBodyCodec());
    registry.registerRequestCodec(new FindCoordinatorRequestBodyCodec());
    registry.registerRequestCodec(new OffsetCommitRequestBodyCodec());
    registry.registerRequestCodec(new OffsetFetchRequestBodyCodec());
    registry.registerRequestCodec(new JoinGroupRequestBodyCodec());
    registry.registerRequestCodec(new HeartbeatRequestBodyCodec());
    registry.registerRequestCodec(new SyncGroupRequestBodyCodec());
    registry.registerResponseCodec(new ApiVersionsResponseBodyCodec());
    registry.registerResponseCodec(new MetadataResponseBodyCodec());
    registry.registerResponseCodec(new ProduceResponseBodyCodec());
    registry.registerResponseCodec(new FetchResponseBodyCodec());
    registry.registerResponseCodec(new ListOffsetsResponseBodyCodec());
    registry.registerResponseCodec(new FindCoordinatorResponseBodyCodec());
    registry.registerResponseCodec(new OffsetCommitResponseBodyCodec());
    registry.registerResponseCodec(new OffsetFetchResponseBodyCodec());
    registry.registerResponseCodec(new JoinGroupResponseBodyCodec());
    registry.registerResponseCodec(new HeartbeatResponseBodyCodec());
    registry.registerResponseCodec(new SyncGroupResponseBodyCodec());
    return registry;
  }

  private record RequestCodecKey(ApiKey apiKey, short apiVersion, Class<?> bodyType) {}

  private record ResponseCodecKey(ApiKey apiKey, short apiVersion) {}
}
