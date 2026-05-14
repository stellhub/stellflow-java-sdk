package io.github.stellhub.stellflow.sdk.client;

import io.github.stellhub.stellflow.sdk.consumer.StellflowConsumerOptions;
import io.github.stellhub.stellflow.sdk.network.BrokerEndpoint;
import io.github.stellhub.stellflow.sdk.network.NettyStellflowClient;
import io.github.stellhub.stellflow.sdk.observability.StellflowObservability;
import io.github.stellhub.stellflow.sdk.producer.ProducerPartitioner;
import io.github.stellhub.stellflow.sdk.producer.StellflowProducerOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Stellflow 客户端统一配置。 */
public record StellflowClientOptions(
    List<BrokerEndpoint> bootstrapServers,
    String clientId,
    int networkThreads,
    int maxFrameLength,
    Duration requestTimeout,
    RetryPolicy retryPolicy,
    StellflowProducerOptions producerOptions,
    StellflowConsumerOptions consumerOptions,
    StellflowObservability observability) {

  public static final String DEFAULT_CLIENT_ID = "stellflow-java-sdk";
  public static final int DEFAULT_NETWORK_THREADS = 1;

  public StellflowClientOptions {
    if (bootstrapServers == null || bootstrapServers.isEmpty()) {
      throw new IllegalArgumentException("bootstrapServers must not be empty");
    }
    bootstrapServers = List.copyOf(bootstrapServers);
    if (clientId == null || clientId.isBlank()) {
      throw new IllegalArgumentException("clientId must not be blank");
    }
    if (networkThreads <= 0) {
      throw new IllegalArgumentException("networkThreads must be positive");
    }
    if (maxFrameLength <= 0) {
      throw new IllegalArgumentException("maxFrameLength must be positive");
    }
    Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
    if (requestTimeout.isZero() || requestTimeout.isNegative()) {
      throw new IllegalArgumentException("requestTimeout must be positive");
    }
    retryPolicy = retryPolicy == null ? RetryPolicy.defaultPolicy() : retryPolicy;
    producerOptions =
        producerOptions == null ? StellflowProducerOptions.defaults() : producerOptions;
    consumerOptions =
        consumerOptions == null ? StellflowConsumerOptions.defaults(clientId) : consumerOptions;
    observability = observability == null ? StellflowObservability.global() : observability;
  }

  /** 创建默认 builder。 */
  public static Builder builder() {
    return new Builder();
  }

  /** 使用 bootstrap.servers 字符串创建 builder。 */
  public static Builder builder(String bootstrapServers) {
    return builder().bootstrapServers(bootstrapServers);
  }

  /** StellflowClientOptions builder。 */
  public static final class Builder {

    private List<BrokerEndpoint> bootstrapServers = List.of();
    private String clientId = DEFAULT_CLIENT_ID;
    private int networkThreads = DEFAULT_NETWORK_THREADS;
    private int maxFrameLength = NettyStellflowClient.DEFAULT_MAX_FRAME_LENGTH;
    private Duration requestTimeout = NettyStellflowClient.DEFAULT_REQUEST_TIMEOUT;
    private RetryPolicy retryPolicy = RetryPolicy.defaultPolicy();
    private StellflowProducerOptions producerOptions = StellflowProducerOptions.defaults();
    private StellflowConsumerOptions consumerOptions;
    private StellflowObservability observability = StellflowObservability.global();

    private Builder() {}

    /** 设置 bootstrap.servers，支持逗号分隔。 */
    public Builder bootstrapServers(String value) {
      if (value == null || value.isBlank()) {
        throw new IllegalArgumentException("bootstrapServers must not be blank");
      }
      String[] values = value.split(",");
      List<String> endpoints = new ArrayList<>(values.length);
      for (String endpoint : values) {
        if (!endpoint.isBlank()) {
          endpoints.add(endpoint.trim());
        }
      }
      return bootstrapServers(endpoints);
    }

    /** 设置 bootstrap.servers。 */
    public Builder bootstrapServers(List<String> values) {
      if (values == null || values.isEmpty()) {
        throw new IllegalArgumentException("bootstrapServers must not be empty");
      }
      this.bootstrapServers = values.stream().map(BrokerEndpoint::parse).toList();
      return this;
    }

    /** 设置 bootstrap endpoint。 */
    public Builder bootstrapEndpoints(List<BrokerEndpoint> values) {
      this.bootstrapServers = List.copyOf(values);
      return this;
    }

    /** 设置 clientId。 */
    public Builder clientId(String value) {
      this.clientId = value;
      return this;
    }

    /** 设置 Netty 网络线程数。 */
    public Builder networkThreads(int value) {
      this.networkThreads = value;
      return this;
    }

    /** 设置最大帧长度。 */
    public Builder maxFrameLength(int value) {
      this.maxFrameLength = value;
      return this;
    }

    /** 设置请求超时。 */
    public Builder requestTimeout(Duration value) {
      this.requestTimeout = value;
      return this;
    }

    /** 设置重试策略。 */
    public Builder retryPolicy(RetryPolicy value) {
      this.retryPolicy = value;
      return this;
    }

    /** 设置 Producer acks。 */
    public Builder producerAcks(short value) {
      this.producerOptions = producerOptions.withAcks(value);
      return this;
    }

    /** 设置 Producer timeoutMs。 */
    public Builder producerTimeoutMs(int value) {
      this.producerOptions = producerOptions.withTimeoutMs(value);
      return this;
    }

    /** 设置 Producer 最大批记录数。 */
    public Builder producerMaxBatchRecords(int value) {
      this.producerOptions = producerOptions.withMaxBatchRecords(value);
      return this;
    }

    /** 设置 Producer 分区器。 */
    public Builder producerPartitioner(ProducerPartitioner value) {
      this.producerOptions = producerOptions.withPartitioner(value);
      return this;
    }

    /** 设置 Producer 配置。 */
    public Builder producerOptions(StellflowProducerOptions value) {
      this.producerOptions = value;
      return this;
    }

    /** 设置 Consumer 配置。 */
    public Builder consumerOptions(StellflowConsumerOptions value) {
      this.consumerOptions = value;
      return this;
    }

    /** 设置可观测性入口。 */
    public Builder observability(StellflowObservability value) {
      this.observability = value;
      return this;
    }

    /** 构建配置。 */
    public StellflowClientOptions build() {
      return new StellflowClientOptions(
          bootstrapServers,
          clientId,
          networkThreads,
          maxFrameLength,
          requestTimeout,
          retryPolicy,
          producerOptions,
          consumerOptions,
          observability);
    }
  }
}
