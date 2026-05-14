package io.github.stellhub.stellflow.sdk.network;

import io.github.stellhub.stellflow.sdk.observability.StellflowObservability;
import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.RequestBody;
import io.github.stellhub.stellflow.sdk.protocol.ResponseMessage;
import io.github.stellhub.stellflow.sdk.protocol.codec.ProtocolCodecRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Broker 长连接池。 */
public class StellflowConnectionPool implements AutoCloseable {

    private static final System.Logger LOGGER =
            System.getLogger(StellflowConnectionPool.class.getName());

    private final ProtocolCodecRegistry registry;
    private final EventLoopGroup eventLoopGroup;
    private final boolean ownsEventLoopGroup;
    private final int maxFrameLength;
    private final Duration requestTimeout;
    private final StellflowObservability observability;
    private final ConcurrentMap<BrokerEndpoint, CompletableFuture<NettyStellflowClient>> clients =
            new ConcurrentHashMap<>();

    public StellflowConnectionPool() {
        this(
                ProtocolCodecRegistry.defaultRegistry(),
                new NioEventLoopGroup(1),
                true,
                NettyStellflowClient.DEFAULT_MAX_FRAME_LENGTH,
                NettyStellflowClient.DEFAULT_REQUEST_TIMEOUT,
                StellflowObservability.global());
    }

    public StellflowConnectionPool(
            ProtocolCodecRegistry registry,
            EventLoopGroup eventLoopGroup,
            boolean ownsEventLoopGroup,
            int maxFrameLength,
            Duration requestTimeout) {
        this(
                registry,
                eventLoopGroup,
                ownsEventLoopGroup,
                maxFrameLength,
                requestTimeout,
                StellflowObservability.global());
    }

    public StellflowConnectionPool(
            ProtocolCodecRegistry registry,
            EventLoopGroup eventLoopGroup,
            boolean ownsEventLoopGroup,
            int maxFrameLength,
            Duration requestTimeout,
            StellflowObservability observability) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.eventLoopGroup = Objects.requireNonNull(eventLoopGroup, "eventLoopGroup must not be null");
        this.ownsEventLoopGroup = ownsEventLoopGroup;
        this.maxFrameLength = maxFrameLength;
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
        this.observability = Objects.requireNonNull(observability, "observability must not be null");
    }

    /** 获取或创建指定 Broker 的连接。 */
    public CompletableFuture<NettyStellflowClient> connect(BrokerEndpoint endpoint) {
        return clients.computeIfAbsent(
                endpoint,
                value -> {
                    LOGGER.log(System.Logger.Level.DEBUG, "Creating Stellflow pooled connection {0}", value);
                    return new NettyStellflowClient(
                                    value,
                                    registry,
                                    eventLoopGroup,
                                    false,
                                    maxFrameLength,
                                    requestTimeout,
                                    observability)
                            .connect();
                });
    }

    /** 发送请求到指定 Broker。 */
    public CompletableFuture<ResponseMessage> send(
            BrokerEndpoint endpoint, ApiKey apiKey, short apiVersion, String clientId, RequestBody body) {
        return connect(endpoint).thenCompose(client -> client.send(apiKey, apiVersion, clientId, body));
    }

    /** 当前已缓存连接快照。 */
    public Map<BrokerEndpoint, CompletableFuture<NettyStellflowClient>> clients() {
        return Map.copyOf(clients);
    }

    /** 返回可观测性入口。 */
    public StellflowObservability observability() {
        return observability;
    }

    @Override
    public void close() {
        for (CompletableFuture<NettyStellflowClient> future : clients.values()) {
            future.thenAccept(NettyStellflowClient::close);
        }
        clients.clear();
        if (ownsEventLoopGroup) {
            eventLoopGroup.shutdownGracefully();
        }
    }
}
