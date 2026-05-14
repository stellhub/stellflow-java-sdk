package io.github.stellhub.stellflow.sdk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.stellhub.stellflow.sdk.admin.ClusterDescription;
import io.github.stellhub.stellflow.sdk.admin.ListOffsetsResult;
import io.github.stellhub.stellflow.sdk.admin.OffsetSpec;
import io.github.stellhub.stellflow.sdk.admin.StellflowAdminClient;
import io.github.stellhub.stellflow.sdk.admin.TopicDescription;
import io.github.stellhub.stellflow.sdk.client.RetryPolicy;
import io.github.stellhub.stellflow.sdk.client.StellflowClientFactory;
import io.github.stellhub.stellflow.sdk.client.StellflowClientOptions;
import io.github.stellhub.stellflow.sdk.consumer.ConsumerGroupSession;
import io.github.stellhub.stellflow.sdk.consumer.ConsumerRebalanceListener;
import io.github.stellhub.stellflow.sdk.consumer.ConsumerRecord;
import io.github.stellhub.stellflow.sdk.consumer.OffsetAndMetadata;
import io.github.stellhub.stellflow.sdk.consumer.StellflowConsumer;
import io.github.stellhub.stellflow.sdk.consumer.StellflowConsumerOptions;
import io.github.stellhub.stellflow.sdk.metadata.MetadataManager;
import io.github.stellhub.stellflow.sdk.metadata.TopicPartition;
import io.github.stellhub.stellflow.sdk.network.BrokerEndpoint;
import io.github.stellhub.stellflow.sdk.network.NettyStellflowClient;
import io.github.stellhub.stellflow.sdk.network.StellflowConnectionPool;
import io.github.stellhub.stellflow.sdk.producer.ProducerRecord;
import io.github.stellhub.stellflow.sdk.producer.RecordMetadata;
import io.github.stellhub.stellflow.sdk.producer.StellflowProducer;
import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.github.stellhub.stellflow.sdk.protocol.ProtocolConstants;
import io.github.stellhub.stellflow.sdk.protocol.codec.BinaryReader;
import io.github.stellhub.stellflow.sdk.protocol.codec.BinaryWriter;
import io.github.stellhub.stellflow.sdk.protocol.message.ApiVersionsRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.ApiVersionsResponseBody;
import io.github.stellhub.stellflow.sdk.protocol.message.FetchPartitionRequest;
import io.github.stellhub.stellflow.sdk.protocol.message.FetchTopicRequest;
import io.github.stellhub.stellflow.sdk.protocol.message.ProducePartitionData;
import io.github.stellhub.stellflow.sdk.protocol.message.ProduceTopicData;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Producer / Consumer 端到端集成测试。 */
class StellflowProducerConsumerIntegrationTest {

  @Test
  void shouldProduceAndFetchThroughNettyClient() throws Exception {
    int port = findFreePort();
    try (MiniBroker broker = MiniBroker.start(port);
        NettyStellflowClient client =
            new NettyStellflowClient(new BrokerEndpoint("127.0.0.1", port))) {
      client.connect().get(10, TimeUnit.SECONDS);
      ApiVersionsResponseBody apiVersions =
          (ApiVersionsResponseBody)
              client
                  .send(
                      ApiKey.API_VERSIONS,
                      ProtocolConstants.DEFAULT_API_VERSION,
                      "stellflow-sdk-it-client",
                      new ApiVersionsRequestBody("stellflow-java-sdk", "0.0.1", List.of()))
                  .get(10, TimeUnit.SECONDS)
                  .body();
      assertEquals("mini-stellflow-broker", apiVersions.brokerSoftwareName());

      StellflowProducer producer = new StellflowProducer(client, "stellflow-sdk-it-producer");
      StellflowConsumer consumer = new StellflowConsumer(client, "stellflow-sdk-it-consumer");

      byte[] key = "order-1001".getBytes(StandardCharsets.UTF_8);
      byte[] value = "{\"amount\":199}".getBytes(StandardCharsets.UTF_8);
      RecordMetadata metadata =
          producer.send(new ProducerRecord("orders", 0, key, value)).get(10, TimeUnit.SECONDS);

      assertEquals("orders", metadata.topic());
      assertEquals(0, metadata.partition());
      assertEquals(0L, metadata.baseOffset());

      List<ConsumerRecord> records =
          consumer.fetch("orders", 0, 0, 1024 * 1024).get(10, TimeUnit.SECONDS);

      assertEquals(1, records.size());
      assertEquals("orders", records.getFirst().topic());
      assertEquals(0, records.getFirst().partition());
      assertEquals(0L, records.getFirst().offset());
      assertArrayEquals(key, records.getFirst().key());
      assertArrayEquals(value, records.getFirst().value());
    }
  }

  @Test
  void shouldRouteProduceFetchCommitOffsetsAndGroupThroughConnectionPool() throws Exception {
    int port = findFreePort();
    BrokerEndpoint endpoint = new BrokerEndpoint("127.0.0.1", port);
    try (MiniBroker broker = MiniBroker.start(port);
        StellflowConnectionPool pool = new StellflowConnectionPool()) {
      MetadataManager metadataManager =
          new MetadataManager(pool, List.of(endpoint), "stellflow-sdk-it-metadata");
      StellflowProducer producer =
          new StellflowProducer(pool, metadataManager, "stellflow-sdk-it-routed-producer");
      StellflowConsumer consumer =
          new StellflowConsumer(pool, metadataManager, "stellflow-sdk-it-routed-consumer");

      List<RecordMetadata> metadata =
          producer
              .send(
                  List.of(
                      new ProducerRecord(
                          "orders",
                          0,
                          "order-0".getBytes(StandardCharsets.UTF_8),
                          "v0".getBytes(StandardCharsets.UTF_8)),
                      new ProducerRecord(
                          "orders",
                          1,
                          "order-1".getBytes(StandardCharsets.UTF_8),
                          "v1".getBytes(StandardCharsets.UTF_8))))
              .get(10, TimeUnit.SECONDS);

      assertEquals(2, metadata.size());
      assertEquals(0, metadata.get(0).partition());
      assertEquals(1, metadata.get(1).partition());

      assertEquals(1, consumer.fetch("orders", 0, 0, 1024 * 1024).get(10, TimeUnit.SECONDS).size());
      assertEquals(1, consumer.fetch("orders", 1, 0, 1024 * 1024).get(10, TimeUnit.SECONDS).size());

      consumer.commitOffset("orders-group", "orders", 0, 1, "processed").get(10, TimeUnit.SECONDS);
      OffsetAndMetadata committed =
          consumer.fetchOffset("orders-group", "orders", 0).get(10, TimeUnit.SECONDS);
      assertEquals(1L, committed.offset());
      assertEquals("processed", committed.metadata());

      ConsumerGroupSession session =
          consumer.joinGroup("orders-group", "", 30_000).get(10, TimeUnit.SECONDS);
      assertEquals("orders-group", session.groupId());
      assertEquals(1, session.generationId());
      assertEquals("member-1", session.memberId());
      consumer.syncGroup(session).get(10, TimeUnit.SECONDS);
      consumer.heartbeat(session).get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void shouldSubscribePollHeartbeatAndCommitThroughHighLevelConsumer() throws Exception {
    int port = findFreePort();
    BrokerEndpoint endpoint = new BrokerEndpoint("127.0.0.1", port);
    try (MiniBroker broker = MiniBroker.start(port);
        StellflowConnectionPool pool = new StellflowConnectionPool()) {
      MetadataManager metadataManager =
          new MetadataManager(pool, List.of(endpoint), "stellflow-sdk-it-metadata");
      StellflowProducer producer =
          new StellflowProducer(pool, metadataManager, "stellflow-sdk-it-poll-producer");
      StellflowConsumerOptions options =
          new StellflowConsumerOptions(
              "orders-poll-group", "", 30_000, Duration.ofMillis(50), 1024 * 1024, "poll-commit");
      StellflowConsumer consumer =
          new StellflowConsumer(
              pool,
              metadataManager,
              "stellflow-sdk-it-poll-consumer",
              options,
              io.github.stellhub.stellflow.sdk.client.RetryPolicy.defaultPolicy(),
              false);

      producer
          .send(
              List.of(
                  new ProducerRecord(
                      "orders",
                      0,
                      "poll-0".getBytes(StandardCharsets.UTF_8),
                      "value-0".getBytes(StandardCharsets.UTF_8)),
                  new ProducerRecord(
                      "orders",
                      1,
                      "poll-1".getBytes(StandardCharsets.UTF_8),
                      "value-1".getBytes(StandardCharsets.UTF_8))))
          .get(10, TimeUnit.SECONDS);

      consumer.subscribe(List.of("orders")).get(10, TimeUnit.SECONDS);
      assertEquals(
          List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 1)),
          consumer.assignment());

      List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(5)).get(10, TimeUnit.SECONDS);

      assertEquals(2, records.size());
      consumer.commitAsync().get(10, TimeUnit.SECONDS);
      assertEquals(
          1L,
          consumer
              .fetchOffset("orders-poll-group", "orders", 0)
              .get(10, TimeUnit.SECONDS)
              .offset());
      assertEquals(
          1L,
          consumer
              .fetchOffset("orders-poll-group", "orders", 1)
              .get(10, TimeUnit.SECONDS)
              .offset());

      Thread.sleep(150);
      consumer.close();
    }
  }

  @Test
  void shouldNotifyRebalanceWhenSubscriptionAssignmentChanges() throws Exception {
    int port = findFreePort();
    BrokerEndpoint endpoint = new BrokerEndpoint("127.0.0.1", port);
    try (MiniBroker broker = MiniBroker.start(port);
        StellflowConnectionPool pool = new StellflowConnectionPool()) {
      MetadataManager metadataManager =
          new MetadataManager(pool, List.of(endpoint), "stellflow-sdk-it-rebalance-metadata");
      StellflowProducer producer =
          new StellflowProducer(pool, metadataManager, "stellflow-sdk-it-rebalance-producer");
      StellflowConsumerOptions options =
          new StellflowConsumerOptions(
              "rebalance-group", "", 30_000, Duration.ofMillis(50), 1024 * 1024, "rebalance");
      StellflowConsumer consumer =
          new StellflowConsumer(
              pool,
              metadataManager,
              "stellflow-sdk-it-rebalance-consumer",
              options,
              RetryPolicy.defaultPolicy(),
              false);
      List<TopicPartition> revoked = new ArrayList<>();
      List<TopicPartition> assigned = new ArrayList<>();
      ConsumerRebalanceListener listener =
          new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
              revoked.addAll(partitions);
            }

            @Override
            public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {
              assigned.addAll(partitions);
            }
          };

      producer
          .send(
              new ProducerRecord(
                  "orders",
                  0,
                  "rebalance-order".getBytes(StandardCharsets.UTF_8),
                  "value".getBytes(StandardCharsets.UTF_8)))
          .get(10, TimeUnit.SECONDS);
      consumer.subscribe(List.of("orders"), listener).get(10, TimeUnit.SECONDS);

      assertEquals(
          List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 1)), assigned);
      assertEquals(List.of(), revoked);

      producer
          .send(
              new ProducerRecord(
                  "payments",
                  0,
                  "rebalance-payment".getBytes(StandardCharsets.UTF_8),
                  "value".getBytes(StandardCharsets.UTF_8)))
          .get(10, TimeUnit.SECONDS);
      consumer.subscribe(List.of("payments"), listener).get(10, TimeUnit.SECONDS);

      assertEquals(
          List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 1)), revoked);
      assertEquals(
          List.of(
              new TopicPartition("orders", 0),
              new TopicPartition("orders", 1),
              new TopicPartition("payments", 0),
              new TopicPartition("payments", 1)),
          assigned);
      assertEquals(
          List.of(new TopicPartition("payments", 0), new TopicPartition("payments", 1)),
          consumer.assignment());

      List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(5)).get(10, TimeUnit.SECONDS);
      assertEquals(1, records.size());
      assertEquals("payments", records.getFirst().topic());
      consumer.commitAsync().get(10, TimeUnit.SECONDS);
      assertEquals(
          1L,
          consumer
              .fetchOffset("rebalance-group", "payments", 0)
              .get(10, TimeUnit.SECONDS)
              .offset());
      consumer.close();
    }
  }

  @Test
  void shouldCreateProducerAndConsumerFromClientFactory() throws Exception {
    int port = findFreePort();
    BrokerEndpoint endpoint = new BrokerEndpoint("127.0.0.1", port);
    try (MiniBroker broker = MiniBroker.start(port);
        StellflowClientFactory factory =
            StellflowClientFactory.create(
                StellflowClientOptions.builder(endpoint.address())
                    .clientId("stellflow-sdk-it-factory")
                    .retryPolicy(RetryPolicy.defaultPolicy())
                    .consumerOptions(
                        new StellflowConsumerOptions(
                            "orders-factory-group",
                            "",
                            30_000,
                            Duration.ofMillis(50),
                            1024 * 1024,
                            "factory-commit"))
                    .build())) {
      StellflowProducer producer = factory.createProducer();
      StellflowConsumer consumer = factory.createConsumer();

      producer
          .send(
              new ProducerRecord(
                  "orders",
                  0,
                  "factory-key".getBytes(StandardCharsets.UTF_8),
                  "factory-value".getBytes(StandardCharsets.UTF_8)))
          .get(10, TimeUnit.SECONDS);

      consumer.subscribe(List.of("orders")).get(10, TimeUnit.SECONDS);
      List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(5)).get(10, TimeUnit.SECONDS);
      consumer.commitSync(Duration.ofSeconds(5));

      assertEquals(1, records.size());
      assertEquals("orders", records.getFirst().topic());
      assertEquals(0, records.getFirst().partition());
      assertEquals(
          1L,
          consumer
              .fetchOffset("orders-factory-group", "orders", 0)
              .get(10, TimeUnit.SECONDS)
              .offset());
      consumer.close();
    }
  }

  @Test
  void shouldDescribeClusterTopicsAndOffsetsFromAdminClient() throws Exception {
    int port = findFreePort();
    BrokerEndpoint endpoint = new BrokerEndpoint("127.0.0.1", port);
    try (MiniBroker broker = MiniBroker.start(port);
        StellflowClientFactory factory =
            StellflowClientFactory.create(
                StellflowClientOptions.builder(endpoint.address())
                    .clientId("stellflow-sdk-it-admin")
                    .build())) {
      StellflowProducer producer = factory.createProducer();
      StellflowAdminClient adminClient = factory.createAdminClient();

      producer
          .send(
              new ProducerRecord(
                  "orders",
                  0,
                  "admin-key".getBytes(StandardCharsets.UTF_8),
                  "admin-value".getBytes(StandardCharsets.UTF_8)))
          .get(10, TimeUnit.SECONDS);

      ApiVersionsResponseBody apiVersions = adminClient.apiVersions().get(10, TimeUnit.SECONDS);
      assertEquals("mini-stellflow-broker", apiVersions.brokerSoftwareName());

      ClusterDescription cluster = adminClient.describeCluster().get(10, TimeUnit.SECONDS);
      assertEquals("mini-cluster", cluster.clusterId());
      assertEquals(0, cluster.controllerId());
      assertEquals(1, cluster.brokers().size());

      List<TopicDescription> topics =
          adminClient.describeTopics(List.of("orders")).get(10, TimeUnit.SECONDS);
      assertEquals(1, topics.size());
      assertEquals("orders", topics.getFirst().topic());
      assertEquals(2, topics.getFirst().partitions().size());

      ListOffsetsResult latest =
          adminClient.listOffsets("orders", 0, OffsetSpec.latest()).get(10, TimeUnit.SECONDS);
      assertEquals("orders", latest.topic());
      assertEquals(0, latest.partition());
      assertEquals(1L, latest.offset());

      Map<TopicPartition, ListOffsetsResult> offsets =
          adminClient
              .listOffsets(Map.of(new TopicPartition("orders", 0), OffsetSpec.earliest()))
              .get(10, TimeUnit.SECONDS);
      assertEquals(0L, offsets.get(new TopicPartition("orders", 0)).offset());
    }
  }

  @Test
  void shouldPartitionAndBatchProducerRecords() throws Exception {
    int port = findFreePort();
    BrokerEndpoint endpoint = new BrokerEndpoint("127.0.0.1", port);
    try (MiniBroker broker = MiniBroker.start(port);
        StellflowClientFactory factory =
            StellflowClientFactory.create(
                StellflowClientOptions.builder(endpoint.address())
                    .clientId("stellflow-sdk-it-batch")
                    .producerMaxBatchRecords(1024)
                    .build())) {
      StellflowProducer producer = factory.createProducer();
      StellflowConsumer consumer = factory.createConsumer();

      List<RecordMetadata> autoPartitionMetadata =
          producer
              .send(
                  List.of(
                      new ProducerRecord(
                          "orders",
                          "batch-key-0".getBytes(StandardCharsets.UTF_8),
                          "auto-0".getBytes(StandardCharsets.UTF_8)),
                      new ProducerRecord(
                          "orders", null, "auto-1".getBytes(StandardCharsets.UTF_8))))
              .get(10, TimeUnit.SECONDS);

      assertEquals(2, autoPartitionMetadata.size());
      assertEquals("orders", autoPartitionMetadata.get(0).topic());
      assertEquals("orders", autoPartitionMetadata.get(1).topic());

      List<RecordMetadata> batchMetadata =
          producer
              .send(
                  List.of(
                      new ProducerRecord(
                          "orders",
                          0,
                          "batch-0".getBytes(StandardCharsets.UTF_8),
                          "value-0".getBytes(StandardCharsets.UTF_8)),
                      new ProducerRecord(
                          "orders",
                          0,
                          "batch-1".getBytes(StandardCharsets.UTF_8),
                          "value-1".getBytes(StandardCharsets.UTF_8))))
              .get(10, TimeUnit.SECONDS);

      assertEquals(2, batchMetadata.size());
      assertEquals(0, batchMetadata.get(0).partition());
      assertEquals(0, batchMetadata.get(1).partition());
      assertEquals(batchMetadata.get(0).baseOffset() + 1, batchMetadata.get(1).baseOffset());

      List<ConsumerRecord> records =
          consumer
              .fetch("orders", 0, batchMetadata.get(0).baseOffset(), 1024 * 1024)
              .get(10, TimeUnit.SECONDS);

      assertEquals(2, records.size());
      assertEquals(batchMetadata.get(0).baseOffset(), records.get(0).offset());
      assertEquals(batchMetadata.get(1).baseOffset(), records.get(1).offset());
      assertArrayEquals("batch-0".getBytes(StandardCharsets.UTF_8), records.get(0).key());
      assertArrayEquals("batch-1".getBytes(StandardCharsets.UTF_8), records.get(1).key());
      consumer.close();
    }
  }

  private static int findFreePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private record BrokerTopicPartition(String topic, int partition) {}

  private static final class MiniBroker implements AutoCloseable {

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Channel channel;

    private MiniBroker(EventLoopGroup bossGroup, EventLoopGroup workerGroup, Channel channel) {
      this.bossGroup = bossGroup;
      this.workerGroup = workerGroup;
      this.channel = channel;
    }

    static MiniBroker start(int port) throws Exception {
      EventLoopGroup bossGroup = new NioEventLoopGroup(1);
      EventLoopGroup workerGroup = new NioEventLoopGroup(1);
      InMemoryBrokerState state = new InMemoryBrokerState(port);
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
                          .addLast(new MiniBrokerHandler(state));
                    }
                  })
              .bind("127.0.0.1", port)
              .sync()
              .channel();
      return new MiniBroker(bossGroup, workerGroup, channel);
    }

    @Override
    public void close() {
      channel.close();
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  private static final class MiniBrokerHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final InMemoryBrokerState state;

    private MiniBrokerHandler(InMemoryBrokerState state) {
      this.state = state;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
      byte[] payload = new byte[msg.readableBytes()];
      msg.readBytes(payload);
      BinaryReader reader = new BinaryReader(payload);
      RequestHeaderView header = readHeader(reader);
      BinaryWriter body = new BinaryWriter();
      if (header.apiKey() == ApiKey.API_VERSIONS) {
        readApiVersionsRequest(reader);
        writeApiVersionsResponse(body);
      } else if (header.apiKey() == ApiKey.METADATA) {
        writeMetadataResponse(body, readMetadataRequest(reader));
      } else if (header.apiKey() == ApiKey.PRODUCE) {
        writeProduceResponse(body, readProduceRequest(reader));
      } else if (header.apiKey() == ApiKey.FETCH) {
        writeFetchResponse(body, readFetchRequest(reader));
      } else if (header.apiKey() == ApiKey.LIST_OFFSETS) {
        writeListOffsetsResponse(body, readListOffsetsRequest(reader));
      } else if (header.apiKey() == ApiKey.FIND_COORDINATOR) {
        readFindCoordinatorRequest(reader);
        writeFindCoordinatorResponse(body);
      } else if (header.apiKey() == ApiKey.OFFSET_COMMIT) {
        writeOffsetCommitResponse(body, readOffsetCommitRequest(reader));
      } else if (header.apiKey() == ApiKey.OFFSET_FETCH) {
        writeOffsetFetchResponse(body, readOffsetFetchRequest(reader));
      } else if (header.apiKey() == ApiKey.JOIN_GROUP) {
        writeJoinGroupResponse(body, readJoinGroupRequest(reader));
      } else if (header.apiKey() == ApiKey.SYNC_GROUP || header.apiKey() == ApiKey.HEARTBEAT) {
        readGroupRequest(reader);
        writerGroupResponse(body);
      } else {
        throw new IllegalArgumentException("unsupported apiKey in mini broker: " + header.apiKey());
      }
      writeResponse(ctx, header.correlationId(), body.toByteArray());
    }

    private RequestHeaderView readHeader(BinaryReader reader) {
      ApiKey apiKey = ApiKey.fromCode(reader.readShort());
      short apiVersion = reader.readShort();
      short headerVersion = reader.readShort();
      int correlationId = reader.readInt();
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
      assertEquals(ProtocolConstants.DEFAULT_API_VERSION, apiVersion);
      assertEquals(ProtocolConstants.HEADER_VERSION, headerVersion);
      return new RequestHeaderView(apiKey, correlationId);
    }

    private List<ProduceTopicData> readProduceRequest(BinaryReader reader) {
      reader.readNullableString();
      reader.readShort();
      reader.readInt();
      return reader.readArray(
          () ->
              new ProduceTopicData(
                  reader.readNullableString(),
                  reader.readArray(
                      () -> new ProducePartitionData(reader.readInt(), reader.readBytes()))));
    }

    private void readApiVersionsRequest(BinaryReader reader) {
      reader.readNullableString();
      reader.readNullableString();
      reader.readStringArray();
    }

    private void writeApiVersionsResponse(BinaryWriter writer) {
      writer.writeInt(11);
      writeApiVersionRange(writer, ApiKey.API_VERSIONS);
      writeApiVersionRange(writer, ApiKey.METADATA);
      writeApiVersionRange(writer, ApiKey.PRODUCE);
      writeApiVersionRange(writer, ApiKey.FETCH);
      writeApiVersionRange(writer, ApiKey.LIST_OFFSETS);
      writeApiVersionRange(writer, ApiKey.OFFSET_COMMIT);
      writeApiVersionRange(writer, ApiKey.OFFSET_FETCH);
      writeApiVersionRange(writer, ApiKey.FIND_COORDINATOR);
      writeApiVersionRange(writer, ApiKey.HEARTBEAT);
      writeApiVersionRange(writer, ApiKey.JOIN_GROUP);
      writeApiVersionRange(writer, ApiKey.SYNC_GROUP);
      writer.writeNullableString("mini-stellflow-broker");
      writer.writeNullableString("0.0.1-test");
      writer.writeStringArray(List.of("fetch.long_poll"));
    }

    private void writeApiVersionRange(BinaryWriter writer, ApiKey apiKey) {
      writer.writeShort(apiKey.code());
      writer.writeShort((short) 0);
      writer.writeShort((short) 0);
    }

    private List<FetchTopicRequest> readFetchRequest(BinaryReader reader) {
      reader.readInt();
      reader.readInt();
      reader.readInt();
      reader.readInt();
      reader.readByte();
      reader.readInt();
      return reader.readArray(
          () ->
              new FetchTopicRequest(
                  reader.readNullableString(),
                  reader.readArray(
                      () ->
                          new FetchPartitionRequest(
                              reader.readInt(),
                              reader.readInt(),
                              reader.readLong(),
                              reader.readLong(),
                              reader.readInt()))));
    }

    private List<String> readMetadataRequest(BinaryReader reader) {
      List<String> topics =
          reader.readArray(
              () -> {
                String topic = reader.readNullableString();
                return topic == null ? "" : topic;
              });
      reader.readBoolean();
      reader.readBoolean();
      reader.readBoolean();
      return topics;
    }

    private void writeMetadataResponse(BinaryWriter writer, List<String> topics) {
      writer.writeNullableString("mini-cluster");
      writer.writeInt(0);
      writer.writeInt(1);
      writer.writeInt(0);
      writer.writeNullableString("127.0.0.1");
      writer.writeInt(state.port());
      writer.writeNullableString(null);
      writer.writeInt(topics.size());
      for (String topic : topics) {
        writer.writeShort(ErrorCode.NONE.code());
        writer.writeNullableString(topic);
        writer.writeBoolean(false);
        writer.writeInt(2);
        for (int partition = 0; partition < 2; partition++) {
          writer.writeShort(ErrorCode.NONE.code());
          writer.writeInt(partition);
          writer.writeInt(0);
          writer.writeInt(0);
          writer.writeIntArray(List.of(0));
          writer.writeIntArray(List.of(0));
          writer.writeIntArray(List.of());
        }
        writer.writeInt(0);
      }
      writer.writeInt(0);
    }

    private void writeProduceResponse(BinaryWriter writer, List<ProduceTopicData> topics) {
      writer.writeInt(topics.size());
      for (ProduceTopicData topic : topics) {
        writer.writeNullableString(topic.topic());
        writer.writeInt(topic.partitions().size());
        for (ProducePartitionData partition : topic.partitions()) {
          long baseOffset = state.append(topic.topic(), partition.partition(), partition.records());
          writer.writeInt(partition.partition());
          writer.writeShort(ErrorCode.NONE.code());
          writer.writeLong(baseOffset);
          writer.writeInt(0);
          writer.writeLong(-1);
          writer.writeLong(0);
        }
      }
    }

    private void writeFetchResponse(BinaryWriter writer, List<FetchTopicRequest> topics) {
      writer.writeInt(0);
      writer.writeInt(topics.size());
      for (FetchTopicRequest topic : topics) {
        writer.writeNullableString(topic.topic());
        writer.writeInt(topic.partitions().size());
        for (FetchPartitionRequest partition : topic.partitions()) {
          InMemoryBrokerState.FetchResult result =
              state.fetch(
                  topic.topic(),
                  partition.partition(),
                  partition.fetchOffset(),
                  partition.partitionMaxBytes());
          writer.writeInt(partition.partition());
          writer.writeShort(ErrorCode.NONE.code());
          writer.writeLong(result.highWatermark());
          writer.writeLong(0);
          writer.writeLong(result.highWatermark());
          writer.writeInt(0);
          writer.writeBytes(result.records());
        }
      }
    }

    private ListOffsetsRequestView readListOffsetsRequest(BinaryReader reader) {
      reader.readInt();
      reader.readByte();
      return new ListOffsetsRequestView(
          reader.readArray(
              () ->
                  new ListOffsetsTopicView(
                      reader.readNullableString(),
                      reader.readArray(
                          () ->
                              new ListOffsetsPartitionView(
                                  reader.readInt(),
                                  reader.readInt(),
                                  reader.readLong(),
                                  reader.readInt())))));
    }

    private void writeListOffsetsResponse(BinaryWriter writer, ListOffsetsRequestView request) {
      writer.writeInt(request.topics().size());
      for (ListOffsetsTopicView topic : request.topics()) {
        writer.writeNullableString(topic.topic());
        writer.writeInt(topic.partitions().size());
        for (ListOffsetsPartitionView partition : topic.partitions()) {
          long offset =
              state.listOffset(topic.topic(), partition.partition(), partition.timestamp());
          writer.writeInt(partition.partition());
          writer.writeShort(ErrorCode.NONE.code());
          writer.writeInt(0);
          writer.writeLong(partition.timestamp());
          writer.writeLong(offset);
          writer.writeLongArray(List.of(offset));
        }
      }
    }

    private void readFindCoordinatorRequest(BinaryReader reader) {
      reader.readNullableString();
      reader.readByte();
    }

    private void writeFindCoordinatorResponse(BinaryWriter writer) {
      writer.writeShort(ErrorCode.NONE.code());
      writer.writeInt(0);
      writer.writeNullableString("127.0.0.1");
      writer.writeInt(state.port());
    }

    private OffsetCommitRequestView readOffsetCommitRequest(BinaryReader reader) {
      String groupId = reader.readNullableString();
      List<OffsetCommitTopicView> topics =
          reader.readArray(
              () ->
                  new OffsetCommitTopicView(
                      reader.readNullableString(),
                      reader.readArray(
                          () ->
                              new OffsetCommitPartitionView(
                                  reader.readInt(),
                                  reader.readLong(),
                                  reader.readNullableString()))));
      return new OffsetCommitRequestView(groupId, topics);
    }

    private void writeOffsetCommitResponse(BinaryWriter writer, OffsetCommitRequestView request) {
      writer.writeInt(request.topics().size());
      for (OffsetCommitTopicView topic : request.topics()) {
        writer.writeNullableString(topic.topic());
        writer.writeInt(topic.partitions().size());
        for (OffsetCommitPartitionView partition : topic.partitions()) {
          state.commit(
              request.groupId(),
              topic.topic(),
              partition.partition(),
              partition.offset(),
              partition.metadata());
          writer.writeInt(partition.partition());
          writer.writeShort(ErrorCode.NONE.code());
        }
      }
    }

    private OffsetFetchRequestView readOffsetFetchRequest(BinaryReader reader) {
      String groupId = reader.readNullableString();
      List<OffsetFetchTopicView> topics =
          reader.readArray(
              () ->
                  new OffsetFetchTopicView(
                      reader.readNullableString(),
                      reader.readArray(() -> new OffsetFetchPartitionView(reader.readInt()))));
      return new OffsetFetchRequestView(groupId, topics);
    }

    private void writeOffsetFetchResponse(BinaryWriter writer, OffsetFetchRequestView request) {
      writer.writeInt(request.topics().size());
      for (OffsetFetchTopicView topic : request.topics()) {
        writer.writeNullableString(topic.topic());
        writer.writeInt(topic.partitions().size());
        for (OffsetFetchPartitionView partition : topic.partitions()) {
          InMemoryBrokerState.OffsetEntry offset =
              state.fetchOffset(request.groupId(), topic.topic(), partition.partition());
          writer.writeInt(partition.partition());
          writer.writeLong(offset.offset());
          writer.writeNullableString(offset.metadata());
          writer.writeShort(ErrorCode.NONE.code());
        }
      }
    }

    private JoinGroupRequestView readJoinGroupRequest(BinaryReader reader) {
      return new JoinGroupRequestView(
          reader.readNullableString(), reader.readNullableString(), reader.readInt());
    }

    private void writeJoinGroupResponse(BinaryWriter writer, JoinGroupRequestView request) {
      String memberId =
          request.memberId() == null || request.memberId().isBlank()
              ? "member-1"
              : request.memberId();
      writer.writeShort(ErrorCode.NONE.code());
      writer.writeInt(1);
      writer.writeNullableString(memberId);
      writer.writeNullableString(memberId);
    }

    private void readGroupRequest(BinaryReader reader) {
      reader.readNullableString();
      reader.readInt();
      reader.readNullableString();
    }

    private void writerGroupResponse(BinaryWriter writer) {
      writer.writeShort(ErrorCode.NONE.code());
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

  private static final class InMemoryBrokerState {

    private final int port;
    private final Map<BrokerTopicPartition, List<byte[]>> records = new HashMap<>();
    private final Map<GroupTopicPartition, OffsetEntry> offsets = new HashMap<>();

    private InMemoryBrokerState(int port) {
      this.port = port;
    }

    int port() {
      return port;
    }

    synchronized long append(String topic, int partition, byte[] value) {
      List<byte[]> partitionRecords =
          records.computeIfAbsent(
              new BrokerTopicPartition(topic, partition), ignored -> new ArrayList<>());
      long baseOffset = partitionRecords.size();
      partitionRecords.add(value.clone());
      return baseOffset;
    }

    synchronized FetchResult fetch(String topic, int partition, long offset, int maxBytes) {
      List<byte[]> partitionRecords =
          records.getOrDefault(new BrokerTopicPartition(topic, partition), List.of());
      BinaryWriter writer = new BinaryWriter();
      int written = 0;
      for (int index = Math.toIntExact(offset); index < partitionRecords.size(); index++) {
        byte[] value = partitionRecords.get(index);
        if (written > 0 && written + value.length > maxBytes) {
          break;
        }
        writer.writeBytesWithoutLength(value);
        written += value.length;
      }
      return new FetchResult(partitionRecords.size(), writer.toByteArray());
    }

    synchronized long listOffset(String topic, int partition, long timestamp) {
      List<byte[]> partitionRecords =
          records.getOrDefault(new BrokerTopicPartition(topic, partition), List.of());
      if (timestamp == OffsetSpec.EARLIEST_TIMESTAMP) {
        return 0;
      }
      return partitionRecords.size();
    }

    synchronized void commit(
        String groupId, String topic, int partition, long offset, String metadata) {
      offsets.put(
          new GroupTopicPartition(groupId, topic, partition), new OffsetEntry(offset, metadata));
    }

    synchronized OffsetEntry fetchOffset(String groupId, String topic, int partition) {
      return offsets.getOrDefault(
          new GroupTopicPartition(groupId, topic, partition), new OffsetEntry(-1, null));
    }

    private record FetchResult(long highWatermark, byte[] records) {}

    private record OffsetEntry(long offset, String metadata) {}
  }

  private record RequestHeaderView(ApiKey apiKey, int correlationId) {}

  private record GroupTopicPartition(String groupId, String topic, int partition) {}

  private record OffsetCommitRequestView(String groupId, List<OffsetCommitTopicView> topics) {}

  private record OffsetCommitTopicView(String topic, List<OffsetCommitPartitionView> partitions) {}

  private record OffsetCommitPartitionView(int partition, long offset, String metadata) {}

  private record OffsetFetchRequestView(String groupId, List<OffsetFetchTopicView> topics) {}

  private record OffsetFetchTopicView(String topic, List<OffsetFetchPartitionView> partitions) {}

  private record OffsetFetchPartitionView(int partition) {}

  private record JoinGroupRequestView(String groupId, String memberId, int sessionTimeoutMs) {}

  private record ListOffsetsRequestView(List<ListOffsetsTopicView> topics) {}

  private record ListOffsetsTopicView(String topic, List<ListOffsetsPartitionView> partitions) {}

  private record ListOffsetsPartitionView(
      int partition, int currentLeaderEpoch, long timestamp, int maxNumOffsets) {}
}
