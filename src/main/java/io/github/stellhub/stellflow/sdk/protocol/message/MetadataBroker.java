package io.github.stellhub.stellflow.sdk.protocol.message;

/** Broker 元数据。 */
public record MetadataBroker(int brokerId, String host, int port, String rack) {}
