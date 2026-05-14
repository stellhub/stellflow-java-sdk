package io.github.stellhub.stellflow.sdk.client;

import io.github.stellhub.stellflow.sdk.admin.StellflowAdminClient;
import io.github.stellhub.stellflow.sdk.consumer.StellflowConsumer;
import io.github.stellhub.stellflow.sdk.consumer.StellflowConsumerOptions;
import io.github.stellhub.stellflow.sdk.metadata.MetadataManager;
import io.github.stellhub.stellflow.sdk.network.BrokerEndpoint;
import io.github.stellhub.stellflow.sdk.network.NettyStellflowClient;
import io.github.stellhub.stellflow.sdk.network.StellflowConnectionPool;
import io.github.stellhub.stellflow.sdk.producer.StellflowProducer;
import io.github.stellhub.stellflow.sdk.protocol.codec.ProtocolCodecRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** Stellflow 客户端工厂。 */
public class StellflowClientFactory implements AutoCloseable {

  private final StellflowClientOptions options;
  private final ProtocolCodecRegistry registry;
  private final EventLoopGroup eventLoopGroup;
  private final StellflowConnectionPool connectionPool;
  private final MetadataManager metadataManager;
  private final AtomicBoolean closed = new AtomicBoolean();

  public StellflowClientFactory(StellflowClientOptions options) {
    this(options, ProtocolCodecRegistry.defaultRegistry());
  }

  public StellflowClientFactory(StellflowClientOptions options, ProtocolCodecRegistry registry) {
    this.options = Objects.requireNonNull(options, "options must not be null");
    this.registry = Objects.requireNonNull(registry, "registry must not be null");
    this.eventLoopGroup = new NioEventLoopGroup(options.networkThreads());
    this.connectionPool =
        new StellflowConnectionPool(
            registry,
            eventLoopGroup,
            true,
            options.maxFrameLength(),
            options.requestTimeout(),
            options.observability());
    this.metadataManager =
        new MetadataManager(connectionPool, options.bootstrapServers(), options.clientId());
  }

  /** 创建默认工厂。 */
  public static StellflowClientFactory create(StellflowClientOptions options) {
    return new StellflowClientFactory(options);
  }

  /** 连接第一个 bootstrap broker，用于预热或 ApiVersions 探测。 */
  public CompletableFuture<NettyStellflowClient> connectBootstrap() {
    ensureOpen();
    return connectionPool.connect(options.bootstrapServers().getFirst());
  }

  /** 创建共享连接池的 Producer。 */
  public StellflowProducer createProducer() {
    return createProducer(options.clientId() + "-producer");
  }

  /** 创建共享连接池的 Producer。 */
  public StellflowProducer createProducer(String clientId) {
    ensureOpen();
    return new StellflowProducer(
        connectionPool,
        metadataManager,
        clientId,
        options.producerOptions(),
        options.retryPolicy(),
        false);
  }

  /** 创建共享连接池的 Consumer。 */
  public StellflowConsumer createConsumer() {
    return createConsumer(options.clientId() + "-consumer", options.consumerOptions());
  }

  /** 创建共享连接池的 Consumer。 */
  public StellflowConsumer createConsumer(StellflowConsumerOptions consumerOptions) {
    return createConsumer(options.clientId() + "-consumer", consumerOptions);
  }

  /** 创建共享连接池的 Consumer。 */
  public StellflowConsumer createConsumer(
      String clientId, StellflowConsumerOptions consumerOptions) {
    ensureOpen();
    return new StellflowConsumer(
        connectionPool, metadataManager, clientId, consumerOptions, options.retryPolicy(), false);
  }

  /** 创建共享连接池的 AdminClient。 */
  public StellflowAdminClient createAdminClient() {
    return createAdminClient(options.clientId() + "-admin");
  }

  /** 创建共享连接池的 AdminClient。 */
  public StellflowAdminClient createAdminClient(String clientId) {
    ensureOpen();
    return new StellflowAdminClient(
        connectionPool, metadataManager, clientId, options.retryPolicy(), false);
  }

  /** 返回配置。 */
  public StellflowClientOptions options() {
    return options;
  }

  /** 返回协议 codec 注册表。 */
  public ProtocolCodecRegistry registry() {
    return registry;
  }

  /** 返回共享连接池。 */
  public StellflowConnectionPool connectionPool() {
    ensureOpen();
    return connectionPool;
  }

  /** 返回共享 MetadataManager。 */
  public MetadataManager metadataManager() {
    ensureOpen();
    return metadataManager;
  }

  /** 返回第一个 bootstrap endpoint。 */
  public BrokerEndpoint bootstrapEndpoint() {
    return options.bootstrapServers().getFirst();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      connectionPool.close();
    }
  }

  private void ensureOpen() {
    if (closed.get()) {
      throw new IllegalStateException("StellflowClientFactory is closed");
    }
  }
}
