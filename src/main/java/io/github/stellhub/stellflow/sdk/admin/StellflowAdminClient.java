package io.github.stellhub.stellflow.sdk.admin;

import io.github.stellhub.stellflow.sdk.client.AsyncRetrier;
import io.github.stellhub.stellflow.sdk.client.RetryPolicy;
import io.github.stellhub.stellflow.sdk.client.StellflowClientException;
import io.github.stellhub.stellflow.sdk.metadata.MetadataManager;
import io.github.stellhub.stellflow.sdk.metadata.TopicPartition;
import io.github.stellhub.stellflow.sdk.network.StellflowConnectionPool;
import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.ProtocolConstants;
import io.github.stellhub.stellflow.sdk.protocol.ResponseMessage;
import io.github.stellhub.stellflow.sdk.protocol.message.ApiVersionsRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.ApiVersionsResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.ListOffsetsPartitionRequest;
import io.github.stellhub.stellflow.sdk.protocol.message.ListOffsetsPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.ListOffsetsRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.ListOffsetsResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.ListOffsetsTopicRequest;
import io.github.stellhub.stellflow.sdk.protocol.message.ListOffsetsTopicResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataTopicResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** Stellflow 管理客户端。 */
public class StellflowAdminClient implements AutoCloseable {

  private static final int CLIENT_REPLICA_ID = -1;
  private static final byte READ_UNCOMMITTED = 0;
  private static final String PRODUCT_NAME = "stellflow-java-sdk";
  private static final String SOFTWARE_VERSION = "0.0.1";

  private final StellflowConnectionPool connectionPool;
  private final MetadataManager metadataManager;
  private final String clientId;
  private final RetryPolicy retryPolicy;
  private final boolean ownsConnectionPool;

  public StellflowAdminClient(
      StellflowConnectionPool connectionPool,
      MetadataManager metadataManager,
      String clientId,
      RetryPolicy retryPolicy,
      boolean ownsConnectionPool) {
    this.connectionPool = Objects.requireNonNull(connectionPool, "connectionPool must not be null");
    this.metadataManager =
        Objects.requireNonNull(metadataManager, "metadataManager must not be null");
    this.clientId = Objects.requireNonNull(clientId, "clientId must not be null");
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
    this.ownsConnectionPool = ownsConnectionPool;
  }

  /** 查询 broker 支持的 API 版本。 */
  public CompletableFuture<ApiVersionsResponseBody> apiVersions() {
    return execute(
        () ->
            connectionPool
                .send(
                    metadataManager.bootstrapEndpoint(),
                    ApiKey.API_VERSIONS,
                    ProtocolConstants.DEFAULT_API_VERSION,
                    clientId,
                    new ApiVersionsRequestBody(PRODUCT_NAME, SOFTWARE_VERSION, List.of()))
                .thenApply(response -> castBody(response, ApiVersionsResponseBody.class)));
  }

  /** 查询原始 Metadata 响应。 */
  public CompletableFuture<MetadataResponseBody> metadata(Collection<String> topics) {
    return execute(() -> metadataManager.refresh(topics));
  }

  /** 查询集群描述。 */
  public CompletableFuture<ClusterDescription> describeCluster() {
    return metadata(List.of()).thenApply(this::toClusterDescription);
  }

  /** 查询 Topic 描述。 */
  public CompletableFuture<List<TopicDescription>> describeTopics(Collection<String> topics) {
    return metadata(topics)
        .thenApply(response -> response.topics().stream().map(this::toTopicDescription).toList());
  }

  /** 查询单个分区 offset。 */
  public CompletableFuture<ListOffsetsResult> listOffsets(
      String topic, int partition, OffsetSpec offsetSpec) {
    return listOffsets(Map.of(new TopicPartition(topic, partition), offsetSpec))
        .thenApply(results -> results.get(new TopicPartition(topic, partition)));
  }

  /** 批量查询分区 offset。 */
  public CompletableFuture<Map<TopicPartition, ListOffsetsResult>> listOffsets(
      Map<TopicPartition, OffsetSpec> requests) {
    if (requests == null || requests.isEmpty()) {
      return CompletableFuture.completedFuture(Map.of());
    }
    Map<TopicPartition, OffsetSpec> orderedRequests = new LinkedHashMap<>(requests);
    List<CompletableFuture<Map.Entry<TopicPartition, ListOffsetsResult>>> futures =
        orderedRequests.entrySet().stream()
            .map(entry -> listOffset(entry.getKey(), entry.getValue()))
            .toList();
    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
        .thenApply(
            ignored -> {
              Map<TopicPartition, ListOffsetsResult> results = new LinkedHashMap<>();
              for (CompletableFuture<Map.Entry<TopicPartition, ListOffsetsResult>> future :
                  futures) {
                Map.Entry<TopicPartition, ListOffsetsResult> entry = future.join();
                results.put(entry.getKey(), entry.getValue());
              }
              return results;
            });
  }

  @Override
  public void close() {
    if (ownsConnectionPool) {
      connectionPool.close();
    }
  }

  private CompletableFuture<Map.Entry<TopicPartition, ListOffsetsResult>> listOffset(
      TopicPartition topicPartition, OffsetSpec offsetSpec) {
    Objects.requireNonNull(topicPartition, "topicPartition must not be null");
    Objects.requireNonNull(offsetSpec, "offsetSpec must not be null");
    return execute(
        () ->
            metadataManager
                .route(topicPartition.topic(), topicPartition.partition())
                .thenCompose(
                    route ->
                        connectionPool.send(
                            route.leaderEndpoint(),
                            ApiKey.LIST_OFFSETS,
                            ProtocolConstants.DEFAULT_API_VERSION,
                            clientId,
                            new ListOffsetsRequestBody(
                                CLIENT_REPLICA_ID,
                                READ_UNCOMMITTED,
                                List.of(
                                    new ListOffsetsTopicRequest(
                                        topicPartition.topic(),
                                        List.of(
                                            new ListOffsetsPartitionRequest(
                                                topicPartition.partition(),
                                                route.leaderEpoch(),
                                                offsetSpec.timestamp(),
                                                1)))))))
                .thenApply(
                    response ->
                        Map.entry(
                            topicPartition,
                            findListOffsetsResult(
                                topicPartition,
                                castBody(response, ListOffsetsResponseBody.class)))));
  }

  private <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> operation) {
    return AsyncRetrier.execute(retryPolicy, operation, this::isRetryable);
  }

  private boolean isRetryable(Throwable throwable) {
    Throwable cause = unwrap(throwable);
    if (cause instanceof StellflowClientException exception) {
      return exception.errorCode() == ErrorCode.LEADER_NOT_AVAILABLE
          || exception.errorCode() == ErrorCode.NOT_LEADER_OR_FOLLOWER
          || exception.errorCode() == ErrorCode.BROKER_NOT_AVAILABLE;
    }
    return cause instanceof IOException || cause instanceof TimeoutException;
  }

  private Throwable unwrap(Throwable throwable) {
    if (throwable instanceof CompletionException && throwable.getCause() != null) {
      return throwable.getCause();
    }
    return throwable;
  }

  private <T> T castBody(ResponseMessage response, Class<T> bodyType) {
    if (response.header().errorCode() != ErrorCode.NONE) {
      throw new StellflowClientException(
          "admin request failed: " + response.header().errorCode(), response.header().errorCode());
    }
    if (!bodyType.isInstance(response.body())) {
      throw new IllegalStateException("unexpected response body: " + response.body().getClass());
    }
    return bodyType.cast(response.body());
  }

  private ClusterDescription toClusterDescription(MetadataResponseBody response) {
    return new ClusterDescription(
        response.clusterId(),
        response.controllerId(),
        response.brokers(),
        response.clusterAuthorizedOperations());
  }

  private TopicDescription toTopicDescription(MetadataTopicResponse topic) {
    return new TopicDescription(
        topic.topic(),
        topic.errorCode(),
        topic.internal(),
        topic.partitions().stream().map(this::toPartitionDescription).toList(),
        topic.topicAuthorizedOperations());
  }

  private PartitionDescription toPartitionDescription(MetadataPartitionResponse partition) {
    return new PartitionDescription(
        partition.partition(),
        partition.errorCode(),
        partition.leaderId(),
        partition.leaderEpoch(),
        partition.replicaNodes(),
        partition.isrNodes(),
        partition.offlineReplicaNodes());
  }

  private ListOffsetsResult findListOffsetsResult(
      TopicPartition topicPartition, ListOffsetsResponseBody response) {
    for (ListOffsetsTopicResponse topic : response.topics()) {
      if (!topicPartition.topic().equals(topic.topic())) {
        continue;
      }
      for (ListOffsetsPartitionResponse partition : topic.partitions()) {
        if (partition.partition() == topicPartition.partition()) {
          if (partition.errorCode() != ErrorCode.NONE) {
            throw new StellflowClientException(
                "list offsets failed: " + partition.errorCode(), partition.errorCode());
          }
          return new ListOffsetsResult(
              topic.topic(),
              partition.partition(),
              partition.errorCode(),
              partition.leaderEpoch(),
              partition.timestamp(),
              partition.offset(),
              partition.offsets());
        }
      }
    }
    throw new IllegalStateException("list offsets response missing " + topicPartition);
  }
}
