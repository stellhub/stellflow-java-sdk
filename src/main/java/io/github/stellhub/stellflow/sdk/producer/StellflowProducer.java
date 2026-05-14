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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
  private final StellflowProducerOptions options;
  private final RetryPolicy retryPolicy;
  private final boolean ownsClient;
  private final boolean ownsConnectionPool;

  public StellflowProducer(NettyStellflowClient client, String clientId) {
    this(client, clientId, StellflowProducerOptions.defaults(), RetryPolicy.defaultPolicy(), false);
  }

  public StellflowProducer(
      NettyStellflowClient client, String clientId, short acks, int timeoutMs, boolean ownsClient) {
    this(
        client,
        clientId,
        StellflowProducerOptions.defaults().withAcks(acks).withTimeoutMs(timeoutMs),
        RetryPolicy.defaultPolicy(),
        ownsClient);
  }

  public StellflowProducer(
      NettyStellflowClient client,
      String clientId,
      short acks,
      int timeoutMs,
      RetryPolicy retryPolicy,
      boolean ownsClient) {
    this(
        client,
        clientId,
        StellflowProducerOptions.defaults().withAcks(acks).withTimeoutMs(timeoutMs),
        retryPolicy,
        ownsClient);
  }

  public StellflowProducer(
      NettyStellflowClient client,
      String clientId,
      StellflowProducerOptions options,
      RetryPolicy retryPolicy,
      boolean ownsClient) {
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.connectionPool = null;
    this.metadataManager = null;
    this.observability = client.observability();
    this.clientId = clientId;
    this.options = Objects.requireNonNull(options, "options must not be null");
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
        StellflowProducerOptions.defaults(),
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
    this(
        connectionPool,
        metadataManager,
        clientId,
        StellflowProducerOptions.defaults().withAcks(acks).withTimeoutMs(timeoutMs),
        retryPolicy,
        ownsConnectionPool);
  }

  public StellflowProducer(
      StellflowConnectionPool connectionPool,
      MetadataManager metadataManager,
      String clientId,
      StellflowProducerOptions options,
      RetryPolicy retryPolicy,
      boolean ownsConnectionPool) {
    this.client = null;
    this.connectionPool = Objects.requireNonNull(connectionPool, "connectionPool must not be null");
    this.metadataManager =
        Objects.requireNonNull(metadataManager, "metadataManager must not be null");
    this.observability = connectionPool.observability();
    this.clientId = clientId;
    this.options = Objects.requireNonNull(options, "options must not be null");
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
    this.ownsClient = false;
    this.ownsConnectionPool = ownsConnectionPool;
  }

  /** 发送单条消息。 */
  public CompletableFuture<RecordMetadata> send(ProducerRecord record) {
    return send(List.of(record)).thenApply(List::getFirst);
  }

  /** 发送多条消息。 */
  public CompletableFuture<List<RecordMetadata>> send(List<ProducerRecord> records) {
    if (records == null || records.isEmpty()) {
      return CompletableFuture.completedFuture(List.of());
    }
    List<ProducerRecord> immutableRecords = List.copyOf(records);
    return AsyncRetrier.execute(
        retryPolicy,
        () -> resolveRecords(immutableRecords).thenCompose(this::sendResolvedOnce),
        throwable -> isRetryable(immutableRecords, throwable));
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

  private CompletableFuture<List<PendingRecord>> resolveRecords(List<ProducerRecord> records) {
    List<CompletableFuture<PendingRecord>> futures = new ArrayList<>(records.size());
    for (int index = 0; index < records.size(); index++) {
      ProducerRecord record = records.get(index);
      int recordIndex = index;
      futures.add(
          resolvePartition(record)
              .thenApply(partition -> new PendingRecord(recordIndex, record, partition)));
    }
    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
        .thenApply(ignored -> futures.stream().map(CompletableFuture::join).toList());
  }

  private CompletableFuture<Integer> resolvePartition(ProducerRecord record) {
    if (record.partition() != ProducerRecord.NO_PARTITION) {
      return CompletableFuture.completedFuture(record.partition());
    }
    if (metadataManager == null) {
      return CompletableFuture.completedFuture(0);
    }
    return metadataManager
        .partitionIds(record.topic())
        .thenApply(
            partitions -> {
              if (partitions.isEmpty()) {
                throw new IllegalStateException("missing topic partitions: " + record.topic());
              }
              return options
                  .partitioner()
                  .partition(record.topic(), record.key(), record.value(), partitions);
            });
  }

  private CompletableFuture<List<RecordMetadata>> sendResolvedOnce(List<PendingRecord> records) {
    List<ProducePartitionBatch> batches = accumulate(records);
    if (metadataManager == null) {
      return sendToDirectClient(batches).thenApply(values -> sortMetadata(values, records.size()));
    }
    List<CompletableFuture<List<IndexedRecordMetadata>>> futures = new ArrayList<>(batches.size());
    for (ProducePartitionBatch batch : batches) {
      futures.add(
          metadataManager
              .route(batch.topic(), batch.partition())
              .thenCompose(route -> sendToLeader(route, batch)));
    }
    return mergeMetadata(futures, records.size());
  }

  private List<ProducePartitionBatch> accumulate(List<PendingRecord> records) {
    Map<PartitionKey, List<PendingRecord>> grouped = new LinkedHashMap<>();
    for (PendingRecord record : records) {
      PartitionKey key = new PartitionKey(record.record().topic(), record.partition());
      grouped.computeIfAbsent(key, ignored -> new ArrayList<>()).add(record);
    }
    List<ProducePartitionBatch> batches = new ArrayList<>(grouped.size());
    for (Map.Entry<PartitionKey, List<PendingRecord>> entry : grouped.entrySet()) {
      batches.add(toPartitionBatch(entry.getKey(), entry.getValue()));
    }
    return batches;
  }

  private ProducePartitionBatch toPartitionBatch(PartitionKey key, List<PendingRecord> records) {
    List<RecordBatchSlice> slices = new ArrayList<>();
    for (int start = 0; start < records.size(); start += options.maxBatchRecords()) {
      int end = Math.min(start + options.maxBatchRecords(), records.size());
      List<PendingRecord> slice = records.subList(start, end);
      slices.add(new RecordBatchSlice(List.copyOf(slice), encodeRecordBatch(slice)));
    }
    byte[] recordBatchSet =
        RecordBatchCodec.encodeBatchSet(slices.stream().map(RecordBatchSlice::batch).toList());
    return new ProducePartitionBatch(
        key.topic(), key.partition(), List.copyOf(slices), recordBatchSet);
  }

  private RecordBatch encodeRecordBatch(List<PendingRecord> records) {
    long now = System.currentTimeMillis();
    List<StellflowRecord> stellflowRecords = new ArrayList<>(records.size());
    for (int index = 0; index < records.size(); index++) {
      ProducerRecord record = records.get(index).record();
      stellflowRecords.add(
          new StellflowRecord((byte) 0, 0, index, record.key(), record.value(), List.of()));
    }
    return RecordBatch.create(
        0, -1, (short) 0, records.size() - 1, now, now, -1, (short) -1, -1, stellflowRecords);
  }

  private CompletableFuture<List<IndexedRecordMetadata>> sendToDirectClient(
      List<ProducePartitionBatch> batches) {
    ProduceRequestBody body = produceRequest(batches);
    return client
        .send(ApiKey.PRODUCE, ProtocolConstants.DEFAULT_API_VERSION, clientId, body)
        .thenApply(response -> toMetadata(batches, response));
  }

  private CompletableFuture<List<IndexedRecordMetadata>> sendToLeader(
      PartitionRoute route, ProducePartitionBatch batch) {
    ProduceRequestBody body = produceRequest(List.of(batch));
    return connectionPool
        .send(
            route.leaderEndpoint(),
            ApiKey.PRODUCE,
            ProtocolConstants.DEFAULT_API_VERSION,
            clientId,
            body)
        .thenApply(response -> toMetadata(List.of(batch), response));
  }

  private ProduceRequestBody produceRequest(List<ProducePartitionBatch> batches) {
    Map<String, List<ProducePartitionData>> byTopic = new LinkedHashMap<>();
    for (ProducePartitionBatch batch : batches) {
      byTopic
          .computeIfAbsent(batch.topic(), ignored -> new ArrayList<>())
          .add(new ProducePartitionData(batch.partition(), batch.recordBatchSet()));
    }
    List<ProduceTopicData> topics = new ArrayList<>(byTopic.size());
    for (Map.Entry<String, List<ProducePartitionData>> entry : byTopic.entrySet()) {
      topics.add(new ProduceTopicData(entry.getKey(), List.copyOf(entry.getValue())));
    }
    return new ProduceRequestBody(null, options.acks(), options.timeoutMs(), topics);
  }

  private List<IndexedRecordMetadata> toMetadata(
      List<ProducePartitionBatch> batches, ResponseMessage response) {
    if (response.header().errorCode() != ErrorCode.NONE) {
      throw new StellflowClientException(
          "produce failed: " + response.header().errorCode(), response.header().errorCode());
    }
    ProduceResponseBody body = (ProduceResponseBody) response.body();
    List<IndexedRecordMetadata> metadata = new ArrayList<>();
    Map<PartitionKey, ProducePartitionResponse> responses = responseByPartition(body);
    for (ProducePartitionBatch batch : batches) {
      ProducePartitionResponse partitionResponse =
          responses.get(new PartitionKey(batch.topic(), batch.partition()));
      if (partitionResponse == null) {
        throw new IllegalStateException("missing produce partition response");
      }
      if (partitionResponse.errorCode() != ErrorCode.NONE) {
        throw new StellflowClientException(
            "produce partition failed: " + partitionResponse.errorCode(),
            partitionResponse.errorCode());
      }
      long offsetBase = partitionResponse.baseOffset();
      for (RecordBatchSlice slice : batch.slices()) {
        for (PendingRecord pending : slice.records()) {
          int offsetDelta = slice.records().indexOf(pending);
          RecordMetadata recordMetadata =
              new RecordMetadata(
                  batch.topic(),
                  batch.partition(),
                  offsetBase + offsetDelta,
                  partitionResponse.currentLeaderEpoch(),
                  partitionResponse.logStartOffset());
          metadata.add(
              new IndexedRecordMetadata(
                  pending.index(), recordSuccess(pending.record(), recordMetadata)));
        }
        offsetBase += slice.records().size();
      }
    }
    return metadata;
  }

  private Map<PartitionKey, ProducePartitionResponse> responseByPartition(
      ProduceResponseBody body) {
    Map<PartitionKey, ProducePartitionResponse> responses = new HashMap<>();
    for (var topicResponse : body.responses()) {
      for (ProducePartitionResponse partitionResponse : topicResponse.partitions()) {
        responses.put(
            new PartitionKey(topicResponse.topic(), partitionResponse.partition()),
            partitionResponse);
      }
    }
    return responses;
  }

  private CompletableFuture<List<RecordMetadata>> mergeMetadata(
      List<CompletableFuture<List<IndexedRecordMetadata>>> futures, int recordCount) {
    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
        .thenApply(
            ignored -> {
              List<IndexedRecordMetadata> indexed = new ArrayList<>();
              for (CompletableFuture<List<IndexedRecordMetadata>> future : futures) {
                indexed.addAll(future.join());
              }
              return sortMetadata(indexed, recordCount);
            });
  }

  private List<RecordMetadata> sortMetadata(List<IndexedRecordMetadata> indexed, int recordCount) {
    RecordMetadata[] metadata = new RecordMetadata[recordCount];
    for (IndexedRecordMetadata value : indexed) {
      metadata[value.index()] = value.metadata();
    }
    List<RecordMetadata> values = new ArrayList<>(recordCount);
    for (RecordMetadata value : metadata) {
      if (value != null) {
        values.add(value);
      }
    }
    return values;
  }

  private RecordMetadata recordSuccess(ProducerRecord record, RecordMetadata metadata) {
    observability.metrics().recordProduced(record.topic(), metadata.partition());
    LOGGER.log(
        System.Logger.Level.DEBUG,
        "Produced Stellflow record topic={0}, partition={1}, offset={2}",
        metadata.topic(),
        metadata.partition(),
        metadata.baseOffset());
    return metadata;
  }

  private boolean isRetryable(List<ProducerRecord> records, Throwable throwable) {
    if (metadataManager != null) {
      for (ProducerRecord record : records) {
        metadataManager.invalidate(record.topic());
      }
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

  private record PendingRecord(int index, ProducerRecord record, int partition) {}

  private record IndexedRecordMetadata(int index, RecordMetadata metadata) {}

  private record PartitionKey(String topic, int partition) {}

  private record RecordBatchSlice(List<PendingRecord> records, RecordBatch batch) {}

  private record ProducePartitionBatch(
      String topic, int partition, List<RecordBatchSlice> slices, byte[] recordBatchSet) {}
}
