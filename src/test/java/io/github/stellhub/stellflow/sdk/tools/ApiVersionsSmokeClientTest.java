package io.github.stellhub.stellflow.sdk.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.stellhub.stellflow.sdk.network.BrokerEndpoint;
import io.github.stellhub.stellflow.sdk.network.NettyStellflowClient;
import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.ProtocolConstants;
import io.github.stellhub.stellflow.sdk.protocol.codec.BinaryReader;
import io.github.stellhub.stellflow.sdk.protocol.codec.BinaryWriter;
import io.github.stellhub.stellflow.sdk.protocol.message.ApiVersionsRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.ApiVersionsResponseBody;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** ApiVersions 冒烟测试。 */
class ApiVersionsSmokeClientTest {

  @Test
  void shouldSendApiVersionsToLocalBroker() throws Exception {
    int port = findFreePort();
    try (ApiVersionsBroker broker = ApiVersionsBroker.start(port);
        NettyStellflowClient client =
            new NettyStellflowClient(new BrokerEndpoint("127.0.0.1", port))) {
      client.connect().get(10, TimeUnit.SECONDS);

      ApiVersionsResponseBody body =
          (ApiVersionsResponseBody)
              client
                  .send(
                      ApiKey.API_VERSIONS,
                      ProtocolConstants.DEFAULT_API_VERSION,
                      "stellflow-java-sdk-smoke",
                      new ApiVersionsRequestBody(
                          "stellflow-java-sdk", "0.0.1", List.of("observability.trace_context")))
                  .get(10, TimeUnit.SECONDS)
                  .body();

      assertEquals("mini-stellflow-broker", body.brokerSoftwareName());
      assertEquals("0.0.1-test", body.brokerSoftwareVersion());
      assertEquals(1, body.apiVersions().size());
      assertEquals(ApiKey.API_VERSIONS.code(), body.apiVersions().getFirst().apiKey());
    }
  }

  private static int findFreePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static final class ApiVersionsBroker implements AutoCloseable {

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Channel channel;

    private ApiVersionsBroker(
        EventLoopGroup bossGroup, EventLoopGroup workerGroup, Channel channel) {
      this.bossGroup = bossGroup;
      this.workerGroup = workerGroup;
      this.channel = channel;
    }

    static ApiVersionsBroker start(int port) throws Exception {
      EventLoopGroup bossGroup = new NioEventLoopGroup(1);
      EventLoopGroup workerGroup = new NioEventLoopGroup(1);
      Channel channel =
          new ServerBootstrap()
              .group(bossGroup, workerGroup)
              .channel(NioServerSocketChannel.class)
              .childOption(ChannelOption.TCP_NODELAY, true)
              .childHandler(
                  new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new LengthFieldBasedFrameDecoder(
                                  NettyStellflowClient.DEFAULT_MAX_FRAME_LENGTH, 0, 4, 0, 4))
                          .addLast(new ApiVersionsHandler());
                    }
                  })
              .bind("127.0.0.1", port)
              .sync()
              .channel();
      return new ApiVersionsBroker(bossGroup, workerGroup, channel);
    }

    @Override
    public void close() {
      channel.close();
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  private static final class ApiVersionsHandler extends SimpleChannelInboundHandler<ByteBuf> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
      byte[] payload = new byte[msg.readableBytes()];
      msg.readBytes(payload);
      BinaryReader reader = new BinaryReader(payload);
      ApiKey apiKey = ApiKey.fromCode(reader.readShort());
      short apiVersion = reader.readShort();
      short headerVersion = reader.readShort();
      int correlationId = reader.readInt();
      readClientMetadata(reader);
      readApiVersionsRequest(reader);

      assertEquals(ApiKey.API_VERSIONS, apiKey);
      assertEquals(ProtocolConstants.DEFAULT_API_VERSION, apiVersion);
      assertEquals(ProtocolConstants.HEADER_VERSION, headerVersion);

      BinaryWriter body = new BinaryWriter();
      body.writeInt(1);
      body.writeShort(ApiKey.API_VERSIONS.code());
      body.writeShort((short) 0);
      body.writeShort((short) 0);
      body.writeNullableString("mini-stellflow-broker");
      body.writeNullableString("0.0.1-test");
      body.writeStringArray(List.of("fetch.long_poll"));
      writeResponse(ctx, correlationId, body.toByteArray());
    }

    private void readClientMetadata(BinaryReader reader) {
      reader.readNullableString();
      reader.readNullableString();
      reader.readNullableString();
      reader.readByte();
      reader.readNullableString();
      reader.readNullableString();
      reader.readNullableString();
      reader.readByte();
      reader.readNullableString();
      reader.readShort();
    }

    private void readApiVersionsRequest(BinaryReader reader) {
      reader.readNullableString();
      reader.readNullableString();
      reader.readStringArray();
    }

    private void writeResponse(ChannelHandlerContext ctx, int correlationId, byte[] body) {
      BinaryWriter payload = new BinaryWriter();
      payload.writeInt(correlationId);
      payload.writeShort(ProtocolConstants.HEADER_VERSION);
      payload.writeShort(ErrorCode.NONE.code());
      payload.writeInt(0);
      payload.writeBytesWithoutLength(body);
      byte[] payloadBytes = payload.toByteArray();

      ByteBuf frame = ctx.alloc().buffer(Integer.BYTES + payloadBytes.length);
      frame.writeInt(payloadBytes.length);
      frame.writeBytes(payloadBytes);
      ctx.writeAndFlush(frame);
    }
  }
}
