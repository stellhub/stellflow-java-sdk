package io.github.stellhub.stellflow.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.github.stellhub.stellflow.sdk.consumer.ConsumerRecord;
import io.github.stellhub.stellflow.sdk.consumer.StellflowConsumer;
import io.github.stellhub.stellflow.sdk.network.BrokerEndpoint;
import io.github.stellhub.stellflow.sdk.network.NettyStellflowClient;
import io.github.stellhub.stellflow.sdk.producer.ProducerRecord;
import io.github.stellhub.stellflow.sdk.producer.RecordMetadata;
import io.github.stellhub.stellflow.sdk.producer.StellflowProducer;
import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ProtocolConstants;
import io.github.stellhub.stellflow.sdk.protocol.message.ApiVersionsRequestBody;
import io.github.stellhub.stellflow.sdk.protocol.message.ApiVersionsResponseBody;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** 本地 Stellflow 服务端调试测试。 */
@Disabled("Requires a local Stellflow broker. Enable manually for local debugging only.")
class LocalStellflowServerDisabledTest {

  private static final String ENDPOINT_PROPERTY = "stellflow.test.endpoint";
  private static final String TOPIC_PROPERTY = "stellflow.test.topic";
  private static final String PARTITION_PROPERTY = "stellflow.test.partition";

  @Test
  void shouldConnectLocalBrokerAndFetchApiVersions() throws Exception {
    try (NettyStellflowClient client = connectClient()) {
      ApiVersionsResponseBody body =
          (ApiVersionsResponseBody)
              client
                  .send(
                      ApiKey.API_VERSIONS,
                      ProtocolConstants.DEFAULT_API_VERSION,
                      "stellflow-sdk-local-debug",
                      new ApiVersionsRequestBody(
                          "stellflow-java-sdk", "0.0.1", List.of("local.debug")))
                  .get(10, TimeUnit.SECONDS)
                  .body();

      assertFalse(body.apiVersions().isEmpty());
    }
  }

  @Test
  void shouldProduceAndFetchAgainstLocalBroker() throws Exception {
    String topic = System.getProperty(TOPIC_PROPERTY, "sdk-local-debug");
    int partition = Integer.getInteger(PARTITION_PROPERTY, 0);
    byte[] key = ("local-key-" + Instant.now().toEpochMilli()).getBytes(StandardCharsets.UTF_8);
    byte[] value = ("local-value-" + Instant.now().toEpochMilli()).getBytes(StandardCharsets.UTF_8);

    try (NettyStellflowClient client = connectClient()) {
      StellflowProducer producer = new StellflowProducer(client, "stellflow-sdk-local-producer");
      StellflowConsumer consumer = new StellflowConsumer(client, "stellflow-sdk-local-consumer");

      RecordMetadata metadata =
          producer.send(new ProducerRecord(topic, partition, key, value)).get(10, TimeUnit.SECONDS);
      List<ConsumerRecord> records =
          consumer
              .fetch(topic, partition, metadata.baseOffset(), 1024 * 1024)
              .get(10, TimeUnit.SECONDS);

      assertFalse(records.isEmpty());
      assertEquals(metadata.baseOffset(), records.getFirst().offset());
    }
  }

  private NettyStellflowClient connectClient() throws Exception {
    BrokerEndpoint endpoint =
        BrokerEndpoint.parse(System.getProperty(ENDPOINT_PROPERTY, "127.0.0.1:9092"));
    NettyStellflowClient client = new NettyStellflowClient(endpoint);
    client.connect().get(10, TimeUnit.SECONDS);
    return client;
  }
}
