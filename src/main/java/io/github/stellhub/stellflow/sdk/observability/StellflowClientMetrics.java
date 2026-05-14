package io.github.stellhub.stellflow.sdk.observability;

import io.github.stellhub.stellflow.sdk.network.BrokerEndpoint;
import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Stellflow 客户端 OpenTelemetry 指标。 */
public final class StellflowClientMetrics {

    private static final AttributeKey<String> API_KEY = AttributeKey.stringKey("stellflow.api_key");
    private static final AttributeKey<String> ENDPOINT = AttributeKey.stringKey("stellflow.endpoint");
    private static final AttributeKey<String> ERROR_CODE =
            AttributeKey.stringKey("stellflow.error_code");
    private static final AttributeKey<String> ERROR_TYPE =
            AttributeKey.stringKey("stellflow.error_type");
    private static final AttributeKey<String> TOPIC =
            AttributeKey.stringKey("messaging.destination.name");
    private static final AttributeKey<Long> PARTITION =
            AttributeKey.longKey("messaging.stellflow.partition");
    private static final AttributeKey<String> GROUP_ID =
            AttributeKey.stringKey("messaging.consumer.group.name");
    private static final AttributeKey<String> GROUP_OPERATION =
            AttributeKey.stringKey("stellflow.group.operation");

    private final LongCounter requests;
    private final LongCounter requestErrors;
    private final DoubleHistogram requestDuration;
    private final LongUpDownCounter inFlightRequests;
    private final LongUpDownCounter connections;
    private final LongCounter producedRecords;
    private final LongCounter fetchedRecords;
    private final LongCounter committedOffsets;
    private final LongCounter groupOperations;

    StellflowClientMetrics(OpenTelemetry openTelemetry) {
        Objects.requireNonNull(openTelemetry, "openTelemetry must not be null");
        Meter meter = openTelemetry.getMeter(StellflowObservability.INSTRUMENTATION_SCOPE);
        this.requests =
                meter
                        .counterBuilder("stellflow.client.requests")
                        .setDescription("Total Stellflow client protocol requests.")
                        .setUnit("{request}")
                        .build();
        this.requestErrors =
                meter
                        .counterBuilder("stellflow.client.request.errors")
                        .setDescription("Total Stellflow client protocol request errors.")
                        .setUnit("{error}")
                        .build();
        this.requestDuration =
                meter
                        .histogramBuilder("stellflow.client.request.duration")
                        .setDescription("Stellflow client request duration.")
                        .setUnit("ms")
                        .build();
        this.inFlightRequests =
                meter
                        .upDownCounterBuilder("stellflow.client.requests.inflight")
                        .setDescription("Current in-flight Stellflow client requests.")
                        .setUnit("{request}")
                        .build();
        this.connections =
                meter
                        .upDownCounterBuilder("stellflow.client.connections")
                        .setDescription("Current active Stellflow client TCP connections.")
                        .setUnit("{connection}")
                        .build();
        this.producedRecords =
                meter
                        .counterBuilder("stellflow.producer.records")
                        .setDescription("Total records produced by Stellflow producer.")
                        .setUnit("{record}")
                        .build();
        this.fetchedRecords =
                meter
                        .counterBuilder("stellflow.consumer.records")
                        .setDescription("Total records fetched by Stellflow consumer.")
                        .setUnit("{record}")
                        .build();
        this.committedOffsets =
                meter
                        .counterBuilder("stellflow.consumer.offset.commits")
                        .setDescription("Total committed Stellflow consumer offsets.")
                        .setUnit("{commit}")
                        .build();
        this.groupOperations =
                meter
                        .counterBuilder("stellflow.consumer.group.operations")
                        .setDescription("Total Stellflow consumer group operations.")
                        .setUnit("{operation}")
                        .build();
    }

    /** 记录请求开始。 */
    public void requestStarted(BrokerEndpoint endpoint, ApiKey apiKey) {
        Attributes attributes = endpointApiAttributes(endpoint, apiKey);
        requests.add(1, attributes);
        inFlightRequests.add(1, attributes);
    }

    /** 记录请求完成。 */
    public void requestCompleted(
            BrokerEndpoint endpoint, ApiKey apiKey, ErrorCode errorCode, long startNanos) {
        Attributes attributes =
                endpointApiAttributes(endpoint, apiKey).toBuilder()
                        .put(ERROR_CODE, errorCode.name())
                        .build();
        requestDuration.record(elapsedMillis(startNanos), attributes);
        inFlightRequests.add(-1, endpointApiAttributes(endpoint, apiKey));
        if (errorCode != ErrorCode.NONE) {
            requestErrors.add(1, attributes);
        }
    }

    /** 记录请求异常。 */
    public void requestFailed(
            BrokerEndpoint endpoint, ApiKey apiKey, Throwable throwable, long startNanos) {
        Attributes attributes =
                endpointApiAttributes(endpoint, apiKey).toBuilder()
                        .put(ERROR_CODE, "exception")
                        .put(ERROR_TYPE, throwable.getClass().getName())
                        .build();
        requestDuration.record(elapsedMillis(startNanos), attributes);
        inFlightRequests.add(-1, endpointApiAttributes(endpoint, apiKey));
        requestErrors.add(1, attributes);
    }

    /** 记录连接打开。 */
    public void connectionOpened(BrokerEndpoint endpoint) {
        connections.add(1, endpointAttributes(endpoint));
    }

    /** 记录连接关闭。 */
    public void connectionClosed(BrokerEndpoint endpoint) {
        connections.add(-1, endpointAttributes(endpoint));
    }

    /** 记录 Producer 成功写入。 */
    public void recordProduced(String topic, int partition) {
        producedRecords.add(1, topicPartitionAttributes(topic, partition));
    }

    /** 记录 Consumer 拉取。 */
    public void recordsFetched(String topic, int partition, int count) {
        if (count > 0) {
            fetchedRecords.add(count, topicPartitionAttributes(topic, partition));
        }
    }

    /** 记录 offset commit。 */
    public void offsetCommitted(String groupId, String topic, int partition) {
        committedOffsets.add(
                1, topicPartitionAttributes(topic, partition).toBuilder().put(GROUP_ID, groupId).build());
    }

    /** 记录消费组操作。 */
    public void groupOperation(String groupId, String operation) {
        groupOperations.add(1, Attributes.of(GROUP_ID, groupId, GROUP_OPERATION, operation));
    }

    private Attributes endpointApiAttributes(BrokerEndpoint endpoint, ApiKey apiKey) {
        return endpointAttributes(endpoint).toBuilder().put(API_KEY, apiKey.name()).build();
    }

    private Attributes endpointAttributes(BrokerEndpoint endpoint) {
        return Attributes.of(ENDPOINT, endpoint.address());
    }

    private Attributes topicPartitionAttributes(String topic, int partition) {
        AttributesBuilder builder = Attributes.builder().put(TOPIC, topic);
        if (partition >= 0) {
            builder.put(PARTITION, partition);
        }
        return builder.build();
    }

    private double elapsedMillis(long startNanos) {
        return (double) (System.nanoTime() - startNanos) / TimeUnit.MILLISECONDS.toNanos(1);
    }
}
