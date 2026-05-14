package io.github.stellhub.stellflow.sdk.network;

import io.github.stellhub.stellflow.sdk.observability.StellflowObservability;
import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.RequestBody;
import io.github.stellhub.stellflow.sdk.protocol.RequestHeader;
import io.github.stellhub.stellflow.sdk.protocol.RequestMessage;
import io.github.stellhub.stellflow.sdk.protocol.ResponseMessage;
import io.github.stellhub.stellflow.sdk.protocol.codec.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.sdk.protocol.codec.ProtocolFrameCodec;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.ScheduledFuture;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/** 基于 Netty 的 Stellflow 数据面客户端。 */
public class NettyStellflowClient implements AutoCloseable {

    public static final int DEFAULT_MAX_FRAME_LENGTH = 64 * 1024 * 1024;
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final System.Logger LOGGER =
            System.getLogger(NettyStellflowClient.class.getName());

    private final BrokerEndpoint endpoint;
    private final ProtocolFrameCodec frameCodec;
    private final EventLoopGroup eventLoopGroup;
    private final boolean ownsEventLoopGroup;
    private final InFlightRequests inFlightRequests = new InFlightRequests();
    private final AtomicInteger correlationIdGenerator = new AtomicInteger();
    private final int maxFrameLength;
    private final Duration requestTimeout;
    private final StellflowObservability observability;

    private volatile Channel channel;

    public NettyStellflowClient(BrokerEndpoint endpoint) {
        this(
                endpoint,
                ProtocolCodecRegistry.defaultRegistry(),
                new NioEventLoopGroup(1),
                true,
                DEFAULT_MAX_FRAME_LENGTH,
                DEFAULT_REQUEST_TIMEOUT,
                StellflowObservability.global());
    }

    public NettyStellflowClient(
            BrokerEndpoint endpoint,
            ProtocolCodecRegistry registry,
            EventLoopGroup eventLoopGroup,
            boolean ownsEventLoopGroup,
            int maxFrameLength,
            Duration requestTimeout) {
        this(
                endpoint,
                registry,
                eventLoopGroup,
                ownsEventLoopGroup,
                maxFrameLength,
                requestTimeout,
                StellflowObservability.global());
    }

    public NettyStellflowClient(
            BrokerEndpoint endpoint,
            ProtocolCodecRegistry registry,
            EventLoopGroup eventLoopGroup,
            boolean ownsEventLoopGroup,
            int maxFrameLength,
            Duration requestTimeout,
            StellflowObservability observability) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
        this.frameCodec =
                new ProtocolFrameCodec(Objects.requireNonNull(registry, "registry must not be null"));
        this.eventLoopGroup = Objects.requireNonNull(eventLoopGroup, "eventLoopGroup must not be null");
        this.ownsEventLoopGroup = ownsEventLoopGroup;
        this.maxFrameLength = maxFrameLength;
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
        this.observability = Objects.requireNonNull(observability, "observability must not be null");
    }

    /** 连接 Broker。 */
    public CompletableFuture<NettyStellflowClient> connect() {
        Channel activeChannel = channel;
        if (activeChannel != null && activeChannel.isActive()) {
            return CompletableFuture.completedFuture(this);
        }

        CompletableFuture<NettyStellflowClient> result = new CompletableFuture<>();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ch.pipeline()
                                        .addLast(new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4))
                                        .addLast(new ResponseDecoder(frameCodec, inFlightRequests))
                                        .addLast(new RequestEncoder(frameCodec));
                            }
                        });

        ChannelFuture connectFuture = bootstrap.connect(endpoint.host(), endpoint.port());
        connectFuture.addListener(
                future -> {
                    if (future.isSuccess()) {
                        channel = connectFuture.channel();
                        observability.metrics().connectionOpened(endpoint);
                        LOGGER.log(System.Logger.Level.DEBUG, "Connected to Stellflow broker {0}", endpoint);
                        channel
                                .closeFuture()
                                .addListener(
                                        ignored -> {
                                            observability.metrics().connectionClosed(endpoint);
                                            LOGGER.log(
                                                    System.Logger.Level.DEBUG,
                                                    "Disconnected from Stellflow broker {0}",
                                                    endpoint);
                                            inFlightRequests.failAll(
                                                    new IllegalStateException("channel closed: " + endpoint.address()));
                                        });
                        result.complete(this);
                    } else {
                        LOGGER.log(
                                System.Logger.Level.WARNING,
                                "Failed to connect to Stellflow broker " + endpoint,
                                future.cause());
                        result.completeExceptionally(future.cause());
                        if (ownsEventLoopGroup) {
                            eventLoopGroup.shutdownGracefully();
                        }
                    }
                });
        return result;
    }

    /** 发送已经构造好的协议请求。 */
    public CompletableFuture<ResponseMessage> send(RequestMessage request) {
        Channel activeChannel = channel;
        if (activeChannel == null || !activeChannel.isActive()) {
            return failedFuture(
                    new IllegalStateException("client is not connected: " + endpoint.address()));
        }

        InFlightRequests.PendingRequest pendingRequest =
                inFlightRequests.register(request, requestTimeout);
        long startNanos = System.nanoTime();
        observability.metrics().requestStarted(endpoint, request.header().apiKey());
        LOGGER.log(
                System.Logger.Level.DEBUG,
                "Sending Stellflow request apiKey={0}, correlationId={1}, endpoint={2}",
                request.header().apiKey(),
                pendingRequest.correlationId(),
                endpoint);
        ScheduledFuture<?> timeoutFuture =
                activeChannel
                        .eventLoop()
                        .schedule(
                                () ->
                                        inFlightRequests.fail(
                                                pendingRequest.correlationId(),
                                                new TimeoutException(
                                                        "request timed out after "
                                                                + requestTimeout
                                                                + ", correlationId="
                                                                + pendingRequest.correlationId())),
                                requestTimeout.toMillis(),
                                TimeUnit.MILLISECONDS);
        pendingRequest
                .future()
                .whenComplete(
                        (response, throwable) -> {
                            timeoutFuture.cancel(false);
                            if (throwable == null) {
                                observability
                                        .metrics()
                                        .requestCompleted(
                                                endpoint,
                                                request.header().apiKey(),
                                                response.header().errorCode(),
                                                startNanos);
                                LOGGER.log(
                                        System.Logger.Level.DEBUG,
                                        "Completed Stellflow request apiKey={0}, correlationId={1}, errorCode={2},"
                                                + " endpoint={3}",
                                        request.header().apiKey(),
                                        pendingRequest.correlationId(),
                                        response.header().errorCode(),
                                        endpoint);
                            } else {
                                observability
                                        .metrics()
                                        .requestFailed(endpoint, request.header().apiKey(), throwable, startNanos);
                                LOGGER.log(
                                        System.Logger.Level.WARNING,
                                        "Failed Stellflow request apiKey="
                                                + request.header().apiKey()
                                                + ", correlationId="
                                                + pendingRequest.correlationId()
                                                + ", endpoint="
                                                + endpoint,
                                        throwable);
                            }
                        });

        activeChannel
                .writeAndFlush(request)
                .addListener(
                        future -> {
                            if (!future.isSuccess()) {
                                inFlightRequests.fail(pendingRequest.correlationId(), future.cause());
                            }
                        });
        return pendingRequest.future();
    }

    /** 构造默认请求头并发送请求。 */
    public CompletableFuture<ResponseMessage> send(
            ApiKey apiKey, short apiVersion, String clientId, RequestBody body) {
        RequestHeader header = RequestHeader.of(apiKey, apiVersion, nextCorrelationId(), clientId);
        return send(new RequestMessage(header, body));
    }

    /** 返回当前 in-flight 数量。 */
    public int inFlightRequestCount() {
        return inFlightRequests.size();
    }

    /** 返回可观测性入口。 */
    public StellflowObservability observability() {
        return observability;
    }

    /** 返回下一个 correlationId。 */
    public int nextCorrelationId() {
        return correlationIdGenerator.updateAndGet(value -> value == Integer.MAX_VALUE ? 1 : value + 1);
    }

    @Override
    public void close() {
        Channel activeChannel = channel;
        if (activeChannel != null) {
            activeChannel.close();
        }
        inFlightRequests.failAll(new IllegalStateException("client closed: " + endpoint.address()));
        if (ownsEventLoopGroup) {
            eventLoopGroup.shutdownGracefully();
        }
    }

    private CompletableFuture<ResponseMessage> failedFuture(Throwable throwable) {
        CompletableFuture<ResponseMessage> result = new CompletableFuture<>();
        result.completeExceptionally(throwable);
        return result;
    }
}
