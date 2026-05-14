package io.github.stellhub.stellflow.sdk.network;

import io.github.stellhub.stellflow.sdk.protocol.ProtocolDecodingException;
import io.github.stellhub.stellflow.sdk.protocol.ResponseMessage;
import io.github.stellhub.stellflow.sdk.protocol.codec.ProtocolFrameCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

/** 响应 payload 解码器。 */
public class ResponseDecoder extends MessageToMessageDecoder<ByteBuf> {

    private final ProtocolFrameCodec frameCodec;
    private final InFlightRequests inFlightRequests;

    public ResponseDecoder(ProtocolFrameCodec frameCodec, InFlightRequests inFlightRequests) {
        this.frameCodec = frameCodec;
        this.inFlightRequests = inFlightRequests;
    }

    /** 根据 correlationId 找到请求上下文并解码响应体。 */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
        byte[] payload = new byte[msg.readableBytes()];
        msg.readBytes(payload);
        int correlationId = readCorrelationId(payload);
        InFlightRequests.PendingRequest pendingRequest =
                inFlightRequests
                        .get(correlationId)
                        .orElseThrow(
                                () ->
                                        new ProtocolDecodingException(
                                                "unknown response correlationId: " + correlationId));
        ResponseMessage response =
                frameCodec.decodeResponsePayload(
                        payload, pendingRequest.apiKey(), pendingRequest.apiVersion());
        inFlightRequests.complete(response);
        out.add(response);
    }

    /** 解码异常时关闭连接并失败所有未完成请求。 */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        inFlightRequests.failAll(cause);
        ctx.close();
    }

    private int readCorrelationId(byte[] payload) {
        if (payload.length < Integer.BYTES) {
            throw new ProtocolDecodingException("response payload is shorter than response header");
        }
        return ((payload[0] & 0xff) << 24)
                | ((payload[1] & 0xff) << 16)
                | ((payload[2] & 0xff) << 8)
                | (payload[3] & 0xff);
    }
}
