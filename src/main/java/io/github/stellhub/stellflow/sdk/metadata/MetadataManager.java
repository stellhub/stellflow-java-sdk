package io.github.stellhub.stellflow.sdk.metadata;

import io.github.stellhub.stellflow.sdk.network.BrokerEndpoint;
import io.github.stellhub.stellflow.sdk.network.StellflowConnectionPool;
import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.ProtocolConstants;
import io.github.stellhub.stellflow.sdk.protocol.ResponseMessage;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataBroker;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataTopicRequest;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataTopicResponse;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Metadata 路由缓存。 */
public class MetadataManager {

  private final StellflowConnectionPool connectionPool;
  private final List<BrokerEndpoint> bootstrapEndpoints;
  private final String clientId;
  private final Map<Integer, BrokerEndpoint> brokers = new HashMap<>();
  private final Map<TopicPartition, PartitionRoute> routes = new HashMap<>();

  public MetadataManager(
      StellflowConnectionPool connectionPool,
      List<BrokerEndpoint> bootstrapEndpoints,
      String clientId) {
    if (bootstrapEndpoints == null || bootstrapEndpoints.isEmpty()) {
      throw new IllegalArgumentException("bootstrap endpoints must not be empty");
    }
    this.connectionPool = connectionPool;
    this.bootstrapEndpoints = List.copyOf(bootstrapEndpoints);
    this.clientId = clientId;
  }

  /** 刷新指定 topic 的 metadata。 */
  public CompletableFuture<MetadataResponseBody> refresh(Collection<String> topics) {
    Set<String> uniqueTopics = new LinkedHashSet<>(topics == null ? List.of() : topics);
    MetadataRequestBody body =
        new MetadataRequestBody(
            uniqueTopics.stream().map(MetadataTopicRequest::new).toList(), false, false, false);
    BrokerEndpoint endpoint = firstKnownBroker();
    return connectionPool
        .send(endpoint, ApiKey.METADATA, ProtocolConstants.DEFAULT_API_VERSION, clientId, body)
        .thenApply(this::applyMetadata);
  }

  /** 查询分区路由，缺失时自动刷新该 topic。 */
  public CompletableFuture<PartitionRoute> route(String topic, int partition) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    PartitionRoute route = routes.get(topicPartition);
    if (route != null) {
      return CompletableFuture.completedFuture(route);
    }
    return refresh(List.of(topic))
        .thenApply(
            ignored ->
                Optional.ofNullable(routes.get(topicPartition))
                    .orElseGet(() -> fallbackRoute(topic, partition)));
  }

  /** 查询 topic 的分区编号列表，缺失时自动刷新 metadata。 */
  public CompletableFuture<List<Integer>> partitionIds(String topic) {
    List<Integer> cached = cachedPartitionIds(topic);
    if (!cached.isEmpty()) {
      return CompletableFuture.completedFuture(cached);
    }
    return refresh(List.of(topic)).thenApply(ignored -> cachedPartitionIds(topic));
  }

  /** 移除指定 topic 的本地路由缓存。 */
  public void invalidate(String topic) {
    routes.keySet().removeIf(topicPartition -> topicPartition.topic().equals(topic));
  }

  /** 返回 bootstrap endpoint。 */
  public BrokerEndpoint bootstrapEndpoint() {
    return bootstrapEndpoints.getFirst();
  }

  private MetadataResponseBody applyMetadata(ResponseMessage response) {
    if (response.header().errorCode() != ErrorCode.NONE) {
      throw new IllegalStateException("metadata refresh failed: " + response.header().errorCode());
    }
    MetadataResponseBody body = (MetadataResponseBody) response.body();
    brokers.clear();
    for (MetadataBroker broker : body.brokers()) {
      brokers.put(broker.brokerId(), new BrokerEndpoint(broker.host(), broker.port()));
    }
    for (MetadataTopicResponse topic : body.topics()) {
      if (topic.errorCode() != ErrorCode.NONE) {
        continue;
      }
      for (MetadataPartitionResponse partition : topic.partitions()) {
        BrokerEndpoint leaderEndpoint = brokers.get(partition.leaderId());
        if (leaderEndpoint == null) {
          continue;
        }
        routes.put(
            new TopicPartition(topic.topic(), partition.partition()),
            new PartitionRoute(
                topic.topic(),
                partition.partition(),
                partition.leaderId(),
                partition.leaderEpoch(),
                leaderEndpoint,
                partition.replicaNodes(),
                partition.isrNodes()));
      }
    }
    return body;
  }

  private BrokerEndpoint firstKnownBroker() {
    return brokers.values().stream().findFirst().orElseGet(this::bootstrapEndpoint);
  }

  private PartitionRoute fallbackRoute(String topic, int partition) {
    return new PartitionRoute(topic, partition, 0, -1, bootstrapEndpoint(), List.of(), List.of());
  }

  private List<Integer> cachedPartitionIds(String topic) {
    return routes.keySet().stream()
        .filter(topicPartition -> topicPartition.topic().equals(topic))
        .map(TopicPartition::partition)
        .sorted(Comparator.naturalOrder())
        .toList();
  }
}
