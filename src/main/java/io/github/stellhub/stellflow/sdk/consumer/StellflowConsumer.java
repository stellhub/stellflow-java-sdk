package io.github.stellhub.stellflow.sdk.consumer;

import io.github.stellhub.stellflow.sdk.client.AsyncRetrier;
import io.github.stellhub.stellflow.sdk.client.RetryPolicy;
import io.github.stellhub.stellflow.sdk.client.StellflowClientException;
import io.github.stellhub.stellflow.sdk.metadata.MetadataManager;
import io.github.stellhub.stellflow.sdk.metadata.PartitionRoute;
import io.github.stellhub.stellflow.sdk.metadata.TopicPartition;
import io.github.stellhub.stellflow.sdk.network.BrokerEndpoint;
import io.github.stellhub.stellflow.sdk.network.NettyStellflowClient;
import io.github.stellhub.stellflow.sdk.network.StellflowConnectionPool;
import io.github.stellhub.stellflow.sdk.observability.StellflowObservability;
import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.ProtocolConstants;
import io.github.stellhub.stellflow.sdk.protocol.RequestBody;
import io.github.stellhub.stellflow.sdk.protocol.ResponseMessage;
import io.github.stellhub.stellflow.sdk.protocol.codec.RecordBatchCodec;
import io.github.stellhub.stellflow.sdk.protocol.message.FetchPartitionRequest;
import io.github.stellhub.stellflow.sdk.protocol.message.FetchPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.FetchRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.FetchResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.FetchTopicRequest;
import io.github.stellhub.stellflow.sdk.protocol.message.FindCoordinatorRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.FindCoordinatorResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.HeartbeatRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.HeartbeatResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.JoinGroupRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.JoinGroupResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.MetadataTopicResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetCommitPartition;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetCommitPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetCommitRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetCommitResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetCommitTopic;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetFetchPartition;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetFetchPartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetFetchRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetFetchResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.OffsetFetchTopic;
import io.github.stellhub.stellflow.sdk.protocol.message.RecordBatch;
import io.github.stellhub.stellflow.sdk.protocol.message.StellflowRecord;
import io.github.stellhub.stellflow.sdk.protocol.message.SyncGroupRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.SyncGroupResponseBody;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Stellflow Consumer 封装。 */
public class StellflowConsumer implements AutoCloseable {

  private static final byte COORDINATOR_KEY_TYPE_GROUP = 0;
  private static final System.Logger LOGGER = System.getLogger(StellflowConsumer.class.getName());

  private final NettyStellflowClient client;
  private final StellflowConnectionPool connectionPool;
  private final MetadataManager metadataManager;
  private final StellflowObservability observability;
  private final String clientId;
  private final StellflowConsumerOptions options;
  private final RetryPolicy retryPolicy;
  private final ScheduledExecutorService heartbeatExecutor;
  private final boolean ownsClient;
  private final boolean ownsConnectionPool;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final Object stateLock = new Object();
  private final Map<TopicPartition, Long> nextOffsets = new HashMap<>();
  private final Map<TopicPartition, Long> consumedOffsets = new HashMap<>();

  private volatile Set<String> subscribedTopics = Set.of();
  private volatile List<TopicPartition> assignment = List.of();
  private volatile ConsumerGroupSession groupSession;
  private volatile ScheduledFuture<?> heartbeatTask;

  public StellflowConsumer(NettyStellflowClient client, String clientId) {
    this(
        client,
        clientId,
        StellflowConsumerOptions.defaults(clientId),
        RetryPolicy.defaultPolicy(),
        false);
  }

  public StellflowConsumer(NettyStellflowClient client, String clientId, boolean ownsClient) {
    this(
        client,
        clientId,
        StellflowConsumerOptions.defaults(clientId),
        RetryPolicy.defaultPolicy(),
        ownsClient);
  }

  public StellflowConsumer(
      NettyStellflowClient client, String clientId, RetryPolicy retryPolicy, boolean ownsClient) {
    this(client, clientId, StellflowConsumerOptions.defaults(clientId), retryPolicy, ownsClient);
  }

  public StellflowConsumer(
      NettyStellflowClient client,
      String clientId,
      StellflowConsumerOptions options,
      RetryPolicy retryPolicy,
      boolean ownsClient) {
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.connectionPool = null;
    this.metadataManager = null;
    this.observability = client.observability();
    this.clientId = Objects.requireNonNull(clientId, "clientId must not be null");
    this.options = Objects.requireNonNull(options, "options must not be null");
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
    this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(this::newHeartbeatThread);
    this.ownsClient = ownsClient;
    this.ownsConnectionPool = false;
  }

  public StellflowConsumer(
      StellflowConnectionPool connectionPool, MetadataManager metadataManager, String clientId) {
    this(
        connectionPool,
        metadataManager,
        clientId,
        StellflowConsumerOptions.defaults(clientId),
        RetryPolicy.defaultPolicy(),
        false);
  }

  public StellflowConsumer(
      StellflowConnectionPool connectionPool,
      MetadataManager metadataManager,
      String clientId,
      RetryPolicy retryPolicy,
      boolean ownsConnectionPool) {
    this(
        connectionPool,
        metadataManager,
        clientId,
        StellflowConsumerOptions.defaults(clientId),
        retryPolicy,
        ownsConnectionPool);
  }

  public StellflowConsumer(
      StellflowConnectionPool connectionPool,
      MetadataManager metadataManager,
      String clientId,
      StellflowConsumerOptions options,
      RetryPolicy retryPolicy,
      boolean ownsConnectionPool) {
    this.client = null;
    this.connectionPool = Objects.requireNonNull(connectionPool, "connectionPool must not be null");
    this.metadataManager =
        Objects.requireNonNull(metadataManager, "metadataManager must not be null");
    this.observability = connectionPool.observability();
    this.clientId = Objects.requireNonNull(clientId, "clientId must not be null");
    this.options = Objects.requireNonNull(options, "options must not be null");
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
    this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(this::newHeartbeatThread);
    this.ownsClient = false;
    this.ownsConnectionPool = ownsConnectionPool;
  }

  /** 订阅 topic 并启动消费组心跳。 */
  public CompletableFuture<Void> subscribe(Collection<String> topics) {
    return subscribe(options.groupId(), topics);
  }

  /** 使用指定 groupId 订阅 topic 并启动消费组心跳。 */
  public CompletableFuture<Void> subscribe(String groupId, Collection<String> topics) {
    ensureOpen();
    if (metadataManager == null) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("subscribe requires metadata routing mode"));
    }
    Set<String> uniqueTopics = validateTopics(topics);
    return joinGroup(groupId, options.memberId(), options.sessionTimeoutMs())
        .thenCompose(
            session ->
                syncGroup(session)
                    .thenCompose(ignored -> loadAssignmentAndOffsets(groupId, uniqueTopics))
                    .thenRun(
                        () -> {
                          synchronized (stateLock) {
                            subscribedTopics = uniqueTopics;
                            groupSession = session;
                          }
                          startHeartbeatLoop(session);
                        }));
  }

  /** 手动指定分区，不加入消费组。 */
  public void assign(Collection<TopicPartition> partitions) {
    ensureOpen();
    if (partitions == null || partitions.isEmpty()) {
      throw new IllegalArgumentException("partitions must not be empty");
    }
    List<TopicPartition> newAssignment = List.copyOf(partitions);
    synchronized (stateLock) {
      stopHeartbeatLoop();
      groupSession = null;
      subscribedTopics = Set.of();
      assignment = newAssignment;
      nextOffsets.clear();
      consumedOffsets.clear();
      for (TopicPartition partition : newAssignment) {
        nextOffsets.put(partition, 0L);
      }
    }
  }

  /** 拉取当前订阅或分配的消息。 */
  public CompletableFuture<List<ConsumerRecord>> poll(Duration timeout) {
    ensureOpen();
    List<TopicPartition> assignedPartitions = assignment;
    if (assignedPartitions.isEmpty()) {
      return CompletableFuture.completedFuture(List.of());
    }
    List<CompletableFuture<List<ConsumerRecord>>> futures =
        new ArrayList<>(assignedPartitions.size());
    for (TopicPartition partition : assignedPartitions) {
      futures.add(
          fetch(
              partition.topic(),
              partition.partition(),
              nextOffset(partition),
              options.fetchMaxBytes()));
    }
    CompletableFuture<List<ConsumerRecord>> result =
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .thenApply(
                ignored -> {
                  List<ConsumerRecord> records = new ArrayList<>();
                  for (CompletableFuture<List<ConsumerRecord>> future : futures) {
                    records.addAll(future.join());
                  }
                  updateConsumedOffsets(records);
                  return records;
                });
    if (timeout == null || timeout.isZero() || timeout.isNegative()) {
      return result;
    }
    return result.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** 同步拉取当前订阅或分配的消息。 */
  public List<ConsumerRecord> pollSync(Duration timeout) {
    try {
      return poll(timeout).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("poll interrupted", exception);
    } catch (Exception exception) {
      throw new IllegalStateException("poll failed", exception);
    }
  }

  /** 异步提交已消费位点。 */
  public CompletableFuture<Void> commitAsync() {
    ConsumerGroupSession session = requireGroupSession();
    Map<TopicPartition, Long> offsets = consumedOffsetSnapshot();
    List<CompletableFuture<Void>> futures = new ArrayList<>(offsets.size());
    for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
      TopicPartition partition = entry.getKey();
      futures.add(
          commitOffset(
              session.groupId(),
              partition.topic(),
              partition.partition(),
              entry.getValue(),
              options.offsetCommitMetadata()));
    }
    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
  }

  /** 同步提交已消费位点。 */
  public void commitSync(Duration timeout) {
    try {
      commitAsync().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("commit interrupted", exception);
    } catch (Exception exception) {
      throw new IllegalStateException("commit failed", exception);
    }
  }

  /** 返回当前分区分配。 */
  public List<TopicPartition> assignment() {
    return assignment;
  }

  /** 返回当前订阅 topic。 */
  public Set<String> subscription() {
    return subscribedTopics;
  }

  /** 从指定分区和 offset 开始拉取。 */
  public CompletableFuture<List<ConsumerRecord>> fetch(
      String topic, int partition, long fetchOffset, int maxBytes) {
    return AsyncRetrier.execute(
        retryPolicy,
        () -> fetchOnce(topic, partition, fetchOffset, maxBytes),
        throwable -> isRetryable(topic, throwable));
  }

  /** 提交消费位点。 */
  public CompletableFuture<Void> commitOffset(
      String groupId, String topic, int partition, long offset, String metadata) {
    OffsetCommitRequestBody body =
        new OffsetCommitRequestBody(
            groupId,
            List.of(
                new OffsetCommitTopic(
                    topic, List.of(new OffsetCommitPartition(partition, offset, metadata)))));
    return sendToCoordinator(groupId, ApiKey.OFFSET_COMMIT, body)
        .thenApply(
            response -> {
              assertTopLevelSuccess("offset commit", response);
              OffsetCommitResponseBody responseBody = (OffsetCommitResponseBody) response.body();
              OffsetCommitPartitionResponse partitionResponse =
                  responseBody.topics().stream()
                      .filter(value -> value.topic().equals(topic))
                      .flatMap(value -> value.partitions().stream())
                      .filter(value -> value.partition() == partition)
                      .findFirst()
                      .orElseThrow(
                          () -> new IllegalStateException("missing offset commit response"));
              if (partitionResponse.errorCode() != ErrorCode.NONE) {
                throw new StellflowClientException(
                    "offset commit failed: " + partitionResponse.errorCode(),
                    partitionResponse.errorCode());
              }
              observability.metrics().offsetCommitted(groupId, topic, partition);
              return null;
            });
  }

  /** 查询已提交消费位点。 */
  public CompletableFuture<OffsetAndMetadata> fetchOffset(
      String groupId, String topic, int partition) {
    OffsetFetchRequestBody body =
        new OffsetFetchRequestBody(
            groupId,
            List.of(new OffsetFetchTopic(topic, List.of(new OffsetFetchPartition(partition)))));
    return sendToCoordinator(groupId, ApiKey.OFFSET_FETCH, body)
        .thenApply(
            response -> {
              assertTopLevelSuccess("offset fetch", response);
              OffsetFetchResponseBody responseBody = (OffsetFetchResponseBody) response.body();
              OffsetFetchPartitionResponse partitionResponse =
                  responseBody.topics().stream()
                      .filter(value -> value.topic().equals(topic))
                      .flatMap(value -> value.partitions().stream())
                      .filter(value -> value.partition() == partition)
                      .findFirst()
                      .orElseThrow(
                          () -> new IllegalStateException("missing offset fetch response"));
              if (partitionResponse.errorCode() != ErrorCode.NONE) {
                throw new StellflowClientException(
                    "offset fetch failed: " + partitionResponse.errorCode(),
                    partitionResponse.errorCode());
              }
              return new OffsetAndMetadata(
                  partitionResponse.offset(), partitionResponse.metadata());
            });
  }

  /** 加入消费组。 */
  public CompletableFuture<ConsumerGroupSession> joinGroup(
      String groupId, String memberId, int sessionTimeoutMs) {
    JoinGroupRequestBody body = new JoinGroupRequestBody(groupId, memberId, sessionTimeoutMs);
    return sendToCoordinator(groupId, ApiKey.JOIN_GROUP, body)
        .thenApply(
            response -> {
              assertTopLevelSuccess("join group", response);
              JoinGroupResponseBody responseBody = (JoinGroupResponseBody) response.body();
              if (responseBody.errorCode() != ErrorCode.NONE) {
                throw new StellflowClientException(
                    "join group failed: " + responseBody.errorCode(), responseBody.errorCode());
              }
              observability.metrics().groupOperation(groupId, "join");
              return new ConsumerGroupSession(
                  groupId,
                  responseBody.generationId(),
                  responseBody.memberId(),
                  responseBody.leaderId());
            });
  }

  /** 同步消费组。 */
  public CompletableFuture<Void> syncGroup(ConsumerGroupSession session) {
    SyncGroupRequestBody body =
        new SyncGroupRequestBody(session.groupId(), session.generationId(), session.memberId());
    return sendToCoordinator(session.groupId(), ApiKey.SYNC_GROUP, body)
        .thenApply(
            response -> {
              assertTopLevelSuccess("sync group", response);
              SyncGroupResponseBody responseBody = (SyncGroupResponseBody) response.body();
              if (responseBody.errorCode() != ErrorCode.NONE) {
                throw new StellflowClientException(
                    "sync group failed: " + responseBody.errorCode(), responseBody.errorCode());
              }
              observability.metrics().groupOperation(session.groupId(), "sync");
              return null;
            });
  }

  /** 向消费组协调者发送心跳。 */
  public CompletableFuture<Void> heartbeat(ConsumerGroupSession session) {
    HeartbeatRequestBody body =
        new HeartbeatRequestBody(session.groupId(), session.generationId(), session.memberId());
    return sendToCoordinator(session.groupId(), ApiKey.HEARTBEAT, body)
        .thenApply(
            response -> {
              assertTopLevelSuccess("heartbeat", response);
              HeartbeatResponseBody responseBody = (HeartbeatResponseBody) response.body();
              if (responseBody.errorCode() != ErrorCode.NONE) {
                throw new StellflowClientException(
                    "heartbeat failed: " + responseBody.errorCode(), responseBody.errorCode());
              }
              observability.metrics().groupOperation(session.groupId(), "heartbeat");
              return null;
            });
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    stopHeartbeatLoop();
    heartbeatExecutor.shutdownNow();
    if (ownsClient) {
      client.close();
    }
    if (ownsConnectionPool) {
      connectionPool.close();
    }
  }

  private CompletableFuture<List<ConsumerRecord>> fetchOnce(
      String topic, int partition, long fetchOffset, int maxBytes) {
    FetchRequestBody body = fetchRequest(topic, partition, fetchOffset, maxBytes);
    if (metadataManager == null) {
      return client
          .send(ApiKey.FETCH, ProtocolConstants.DEFAULT_API_VERSION, clientId, body)
          .thenApply(
              response ->
                  recordFetch(
                      topic, partition, toRecords(topic, partition, fetchOffset, response)));
    }
    return metadataManager
        .route(topic, partition)
        .thenCompose(route -> fetchFromLeader(route, topic, partition, fetchOffset, body));
  }

  private CompletableFuture<List<ConsumerRecord>> fetchFromLeader(
      PartitionRoute route, String topic, int partition, long fetchOffset, FetchRequestBody body) {
    return connectionPool
        .send(
            route.leaderEndpoint(),
            ApiKey.FETCH,
            ProtocolConstants.DEFAULT_API_VERSION,
            clientId,
            body)
        .thenApply(
            response ->
                recordFetch(topic, partition, toRecords(topic, partition, fetchOffset, response)));
  }

  private FetchRequestBody fetchRequest(
      String topic, int partition, long fetchOffset, int maxBytes) {
    return new FetchRequestBody(
        -1,
        500,
        1,
        maxBytes,
        (byte) 0,
        0,
        List.of(
            new FetchTopicRequest(
                topic,
                List.of(new FetchPartitionRequest(partition, -1, fetchOffset, -1, maxBytes)))));
  }

  private CompletableFuture<ResponseMessage> sendToCoordinator(
      String groupId, ApiKey apiKey, RequestBody body) {
    if (metadataManager == null) {
      return client.send(apiKey, ProtocolConstants.DEFAULT_API_VERSION, clientId, body);
    }
    return findCoordinator(groupId)
        .thenCompose(
            endpoint ->
                connectionPool.send(
                    endpoint, apiKey, ProtocolConstants.DEFAULT_API_VERSION, clientId, body));
  }

  private CompletableFuture<BrokerEndpoint> findCoordinator(String groupId) {
    FindCoordinatorRequestBody body =
        new FindCoordinatorRequestBody(groupId, COORDINATOR_KEY_TYPE_GROUP);
    return connectionPool
        .send(
            metadataManager.bootstrapEndpoint(),
            ApiKey.FIND_COORDINATOR,
            ProtocolConstants.DEFAULT_API_VERSION,
            clientId,
            body)
        .thenApply(
            response -> {
              assertTopLevelSuccess("find coordinator", response);
              FindCoordinatorResponseBody responseBody =
                  (FindCoordinatorResponseBody) response.body();
              if (responseBody.errorCode() != ErrorCode.NONE) {
                throw new StellflowClientException(
                    "find coordinator failed: " + responseBody.errorCode(),
                    responseBody.errorCode());
              }
              return new BrokerEndpoint(responseBody.host(), responseBody.port());
            });
  }

  private List<ConsumerRecord> toRecords(
      String topic, int partition, long fetchOffset, ResponseMessage response) {
    if (response.header().errorCode() != ErrorCode.NONE) {
      throw new StellflowClientException(
          "fetch failed: " + response.header().errorCode(), response.header().errorCode());
    }
    FetchResponseBody body = (FetchResponseBody) response.body();
    FetchPartitionResponse partitionResponse =
        body.responses().stream()
            .filter(topicResponse -> topicResponse.topic().equals(topic))
            .flatMap(topicResponse -> topicResponse.partitions().stream())
            .filter(value -> value.partition() == partition)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("missing fetch partition response"));
    if (partitionResponse.errorCode() != ErrorCode.NONE) {
      throw new StellflowClientException(
          "fetch partition failed: " + partitionResponse.errorCode(),
          partitionResponse.errorCode());
    }
    if (partitionResponse.records() == null || partitionResponse.records().length == 0) {
      return List.of();
    }
    List<ConsumerRecord> records = new ArrayList<>();
    for (RecordBatch batch : RecordBatchCodec.decodeBatchSet(partitionResponse.records())) {
      for (StellflowRecord record : batch.records()) {
        records.add(
            new ConsumerRecord(
                topic,
                partition,
                fetchOffset + batch.baseOffsetDelta() + record.offsetDelta(),
                record.key(),
                record.value(),
                batch.baseTimestamp() + record.timestampDelta()));
      }
    }
    return records;
  }

  private CompletableFuture<Void> loadAssignmentAndOffsets(
      String groupId, Set<String> uniqueTopics) {
    return metadataManager
        .refresh(uniqueTopics)
        .thenCompose(
            metadata -> {
              List<TopicPartition> partitions = partitionsFromMetadata(metadata.topics());
              List<CompletableFuture<OffsetState>> futures = new ArrayList<>(partitions.size());
              for (TopicPartition partition : partitions) {
                futures.add(
                    fetchOffset(groupId, partition.topic(), partition.partition())
                        .thenApply(
                            offset -> new OffsetState(partition, Math.max(0, offset.offset()))));
              }
              return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                  .thenRun(
                      () -> {
                        synchronized (stateLock) {
                          assignment = partitions;
                          nextOffsets.clear();
                          consumedOffsets.clear();
                          for (CompletableFuture<OffsetState> future : futures) {
                            OffsetState offset = future.join();
                            nextOffsets.put(offset.partition(), offset.offset());
                          }
                        }
                      });
            });
  }

  private List<TopicPartition> partitionsFromMetadata(List<MetadataTopicResponse> topics) {
    List<TopicPartition> partitions = new ArrayList<>();
    for (MetadataTopicResponse topic : topics) {
      if (topic.errorCode() != ErrorCode.NONE) {
        continue;
      }
      for (MetadataPartitionResponse partition : topic.partitions()) {
        if (partition.errorCode() == ErrorCode.NONE) {
          partitions.add(new TopicPartition(topic.topic(), partition.partition()));
        }
      }
    }
    return List.copyOf(partitions);
  }

  private void assertTopLevelSuccess(String operation, ResponseMessage response) {
    if (response.header().errorCode() != ErrorCode.NONE) {
      throw new StellflowClientException(
          operation + " failed: " + response.header().errorCode(), response.header().errorCode());
    }
  }

  private boolean isRetryable(String topic, Throwable throwable) {
    if (metadataManager != null) {
      metadataManager.invalidate(topic);
    }
    if (throwable instanceof StellflowClientException exception) {
      return switch (exception.errorCode()) {
        case BROKER_NOT_AVAILABLE,
            LEADER_NOT_AVAILABLE,
            NOT_LEADER_OR_FOLLOWER,
            UNKNOWN_TOPIC_OR_PARTITION ->
            true;
        default -> false;
      };
    }
    return true;
  }

  private List<ConsumerRecord> recordFetch(
      String topic, int partition, List<ConsumerRecord> records) {
    observability.metrics().recordsFetched(topic, partition, records.size());
    LOGGER.log(
        System.Logger.Level.DEBUG,
        "Fetched Stellflow records topic={0}, partition={1}, count={2}",
        topic,
        partition,
        records.size());
    return records;
  }

  private long nextOffset(TopicPartition partition) {
    synchronized (stateLock) {
      return nextOffsets.getOrDefault(partition, 0L);
    }
  }

  private void updateConsumedOffsets(List<ConsumerRecord> records) {
    synchronized (stateLock) {
      for (ConsumerRecord record : records) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        long nextOffset = record.offset() + 1;
        nextOffsets.merge(partition, nextOffset, Math::max);
        consumedOffsets.merge(partition, nextOffset, Math::max);
      }
    }
  }

  private Map<TopicPartition, Long> consumedOffsetSnapshot() {
    synchronized (stateLock) {
      return Map.copyOf(consumedOffsets);
    }
  }

  private ConsumerGroupSession requireGroupSession() {
    ConsumerGroupSession session = groupSession;
    if (session == null) {
      throw new IllegalStateException("consumer has not joined a group");
    }
    return session;
  }

  private void startHeartbeatLoop(ConsumerGroupSession session) {
    synchronized (stateLock) {
      stopHeartbeatLoop();
      long intervalMs = options.heartbeatInterval().toMillis();
      heartbeatTask =
          heartbeatExecutor.scheduleAtFixedRate(
              () -> sendHeartbeatSafely(session), intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }
  }

  private void stopHeartbeatLoop() {
    ScheduledFuture<?> task = heartbeatTask;
    if (task != null) {
      task.cancel(false);
      heartbeatTask = null;
    }
  }

  private void sendHeartbeatSafely(ConsumerGroupSession session) {
    if (closed.get() || groupSession != session) {
      return;
    }
    heartbeat(session)
        .exceptionally(
            throwable -> {
              LOGGER.log(
                  System.Logger.Level.WARNING,
                  "Failed Stellflow heartbeat groupId=" + session.groupId(),
                  throwable);
              return null;
            });
  }

  private Thread newHeartbeatThread(Runnable runnable) {
    Thread thread = new Thread(runnable, "stellflow-consumer-heartbeat-" + clientId);
    thread.setDaemon(true);
    return thread;
  }

  private Set<String> validateTopics(Collection<String> topics) {
    if (topics == null || topics.isEmpty()) {
      throw new IllegalArgumentException("topics must not be empty");
    }
    Set<String> uniqueTopics = new LinkedHashSet<>();
    for (String topic : topics) {
      if (topic == null || topic.isBlank()) {
        throw new IllegalArgumentException("topic must not be blank");
      }
      uniqueTopics.add(topic);
    }
    return Set.copyOf(uniqueTopics);
  }

  private void ensureOpen() {
    if (closed.get()) {
      throw new IllegalStateException("consumer is closed");
    }
  }

  private record OffsetState(TopicPartition partition, long offset) {}
}
