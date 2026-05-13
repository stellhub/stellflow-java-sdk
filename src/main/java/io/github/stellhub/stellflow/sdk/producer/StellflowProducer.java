package io.github.stellhub.stellflow.sdk.producer;

import io.github.stellhub.stellflow.sdk.client.AsyncRetrier;
import io.github.stellhub.stellflow.sdk.client.RetryPolicy;
import io.github.stellhub.stellflow.sdk.client.StellflowClientException;
import io.github.stellhub.stellflow.sdk.metadata.MetadataManager;
import io.github.stellhub.stellflow.sdk.metadata.PartitionRoute;
import io.github.stellhub.stellflow.sdk.network.NettyStellflowClient;
import io.github.stellhub.stellflow.sdk.network.StellflowConnectionPool;
import io.github.stellhub.stellflow.sdk.observability.StellflowObservability;
import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.ProtocolConstants;
import io.github.stellhub.stellflow.sdk.protocol.ResponseMessage;
import io.github.stellhub.stellflow.sdk.protocol.codec.RecordBatchCodec;
import io.github.stellhub.stellflow.sdk.protocol.message.ProducePartitionData;
import io.github.stellhub.stellflow.sdk.protocol.message.ProducePartitionResponse;
import io.github.stellhub.stellflow.sdk.protocol.message.ProduceRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.ProduceResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.ProduceTopicData;
import io.github.stellhub.stellflow.sdk.protocol.message.RecordBatch;
import io.github.stellhub.stellflow.sdk.protocol.message.StellflowRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Stellflow Producer 封装。 */
public class StellflowProducer implements AutoCloseable {

  private static final System.Logger LOGGER = System.getLogger(StellflowProducer.class.getName());

  private final NettyStellflowClient client;
  private final StellflowConnectionPool connectionPool;
  private final MetadataManager metadataManager;
  private final StellflowObservability observability;
  private final String clientId;
  private final short acks;
  private final int timeoutMs;
  private final RetryPolicy retryPolicy;
  private final boolean ownsClient;
  private final boolean ownsConnectionPool;

  public StellflowProducer(NettyStellflowClient client, String clientId) {
    this(client, clientId, (short) -1, 30_000, RetryPolicy.defaultPolicy(), false);
  }

  public StellflowProducer(
      NettyStellflowClient client, String clientId, short acks, int timeoutMs, boolean ownsClient) {
    this(client, clientId, acks, timeoutMs, RetryPolicy.defaultPolicy(), ownsClient);
  }

  public StellflowProducer(
      NettyStellflowClient client,
      String clientId,
      short acks,
      int timeoutMs,
      RetryPolicy retryPolicy,
      boolean ownsClient) {
    this.client = client;
    this.connectionPool = null;
    this.metadataManager = null;
    this.observability = Objects.requireNonNull(client, "client must not be null").observability();
    this.clientId = clientId;
    this.acks = acks;
    this.timeoutMs = timeoutMs;
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
    this.ownsClient = ownsClient;
    this.ownsConnectionPool = false;
  }

  public StellflowProducer(
      StellflowConnectionPool connectionPool, MetadataManager metadataManager, String clientId) {
    this(
        connectionPool,
        metadataManager,
        clientId,
        (short) -1,
        30_000,
        RetryPolicy.defaultPolicy(),
        false);
  }

  public StellflowProducer(
      StellflowConnectionPool connectionPool,
      MetadataManager metadataManager,
      String clientId,
      short acks,
      int timeoutMs,
      RetryPolicy retryPolicy,
      boolean ownsConnectionPool) {
    this.client = null;
    this.connectionPool = Objects.requireNonNull(connectionPool, "connectionPool must not be null");
    this.metadataManager =
        Objects.requireNonNull(metadataManager, "metadataManager must not be null");
    this.observability = connectionPool.observability();
    this.clientId = clientId;
    this.acks = acks;
    this.timeoutMs = timeoutMs;
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
    this.ownsClient = false;
    this.ownsConnectionPool = ownsConnectionPool;
  }

  /** 发送单条消息。 */
  public CompletableFuture<RecordMetadata> send(ProducerRecord record) {
    return AsyncRetrier.execute(
        retryPolicy, () -> sendOnce(record), throwable -> isRetryable(record.topic(), throwable));
  }

  /** 发送多条消息。 */
  public CompletableFuture<List<RecordMetadata>> send(List<ProducerRecord> records) {
    if (records == null || records.isEmpty()) {
      return CompletableFuture.completedFuture(List.of());
    }
    List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>(records.size());
    for (ProducerRecord record : records) {
      futures.add(send(record));
    }
    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
        .thenApply(ignored -> futures.stream().map(CompletableFuture::join).toList());
  }

  @Override
  public void close() {
    if (ownsClient) {
      client.close();
    }
    if (ownsConnectionPool) {
      connectionPool.close();
    }
  }

  private CompletableFuture<RecordMetadata> sendOnce(ProducerRecord record) {
    ProduceRequestBody body = produceRequest(record);
    if (metadataManager == null) {
      return client
          .send(ApiKey.PRODUCE, ProtocolConstants.DEFAULT_API_VERSION, clientId, body)
          .thenApply(response -> recordSuccess(record, toMetadata(record, response)));
    }
    return metadataManager
        .route(record.topic(), record.partition())
        .thenCompose(route -> sendToLeader(route, record, body));
  }

  private CompletableFuture<RecordMetadata> sendToLeader(
      PartitionRoute route, ProducerRecord record, ProduceRequestBody body) {
    return connectionPool
        .send(
            route.leaderEndpoint(),
            ApiKey.PRODUCE,
            ProtocolConstants.DEFAULT_API_VERSION,
            clientId,
            body)
        .thenApply(response -> recordSuccess(record, toMetadata(record, response)));
  }

  private ProduceRequestBody produceRequest(ProducerRecord record) {
    byte[] recordBatchSet = encodeRecord(record);
    return new ProduceRequestBody(
        null,
        acks,
        timeoutMs,
        List.of(
            new ProduceTopicData(
                record.topic(),
                List.of(new ProducePartitionData(record.partition(), recordBatchSet)))));
  }

  private byte[] encodeRecord(ProducerRecord record) {
    long now = System.currentTimeMillis();
    RecordBatch batch =
        RecordBatch.create(
            0,
            -1,
            (short) 0,
            0,
            now,
            now,
            -1,
            (short) -1,
            -1,
            List.of(new StellflowRecord((byte) 0, 0, 0, record.key(), record.value(), List.of())));
    return RecordBatchCodec.encodeBatchSet(List.of(batch));
  }

  private RecordMetadata toMetadata(ProducerRecord record, ResponseMessage response) {
    if (response.header().errorCode() != ErrorCode.NONE) {
      throw new StellflowClientException(
          "produce failed: " + response.header().errorCode(), response.header().errorCode());
    }
    ProduceResponseBody body = (ProduceResponseBody) response.body();
    ProducePartitionResponse partitionResponse =
        body.responses().stream()
            .filter(topic -> topic.topic().equals(record.topic()))
            .flatMap(topic -> topic.partitions().stream())
            .filter(partition -> partition.partition() == record.partition())
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("missing produce partition response"));
    if (partitionResponse.errorCode() != ErrorCode.NONE) {
      throw new StellflowClientException(
          "produce partition failed: " + partitionResponse.errorCode(),
          partitionResponse.errorCode());
    }
    return new RecordMetadata(
        record.topic(),
        record.partition(),
        partitionResponse.baseOffset(),
        partitionResponse.currentLeaderEpoch(),
        partitionResponse.logStartOffset());
  }

  private RecordMetadata recordSuccess(ProducerRecord record, RecordMetadata metadata) {
    observability.metrics().recordProduced(record.topic(), record.partition());
    LOGGER.log(
        System.Logger.Level.DEBUG,
        "Produced Stellflow record topic={0}, partition={1}, offset={2}",
        metadata.topic(),
        metadata.partition(),
        metadata.baseOffset());
    return metadata;
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
}
