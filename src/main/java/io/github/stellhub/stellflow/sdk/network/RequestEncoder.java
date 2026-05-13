package io.github.stellhub.stellflow.sdk.network;

import io.github.stellhub.stellflow.sdk.protocol.RequestMessage;
import io.github.stellhub.stellflow.sdk.protocol.codec.ProtocolFrameCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/** 请求帧编码器。 */
public class RequestEncoder extends MessageToByteEncoder<RequestMessage> {

  private final ProtocolFrameCodec frameCodec;

  public RequestEncoder(ProtocolFrameCodec frameCodec) {
    this.frameCodec = frameCodec;
  }

  /** 编码请求为 frameLength + header + body。 */
  @Override
  protected void encode(ChannelHandlerContext ctx, RequestMessage msg, ByteBuf out) {
    out.writeBytes(frameCodec.encodeRequest(msg));
  }
}
