package io.github.stellhub.stellflow.sdk.protocol.codec;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.RequestHeader;
import io.github.stellhub.stellflow.sdk.protocol.ResponseHeader;

/** 协议头编解码器。 */
public final class HeaderCodec {

    private HeaderCodec() {}

    /** 编码请求头。 */
    public static void encodeRequestHeader(BinaryWriter writer, RequestHeader header) {
        writer.writeShort(header.apiKeyCode());
        writer.writeShort(header.apiVersion());
        writer.writeShort(header.headerVersion());
        writer.writeInt(header.correlationId());
        writer.writeNullableString(header.clientId());
        writer.writeNullableString(header.traceId());
        writer.writeNullableString(header.spanId());
        writer.writeByte(header.traceFlags());
        writer.writeNullableString(header.tenantId());
        writer.writeNullableString(header.quotaKey());
        writer.writeNullableString(header.authContextId());
        writer.writeByte(header.trafficClass());
        writer.writeNullableString(header.trafficTag());
        writer.writeShort(header.flags());
    }

    /** 解码响应头。 */
    public static ResponseHeader decodeResponseHeader(BinaryReader reader) {
        return new ResponseHeader(
                reader.readInt(),
                reader.readShort(),
                ErrorCode.fromCode(reader.readShort()),
                reader.readInt());
    }
}
