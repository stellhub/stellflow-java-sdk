package io.github.stellhub.stellflow.sdk.network;

import java.net.URI;
import java.util.Objects;

/** Broker 数据面地址。 */
public record BrokerEndpoint(String host, int port) {

  public BrokerEndpoint {
    if (host == null || host.isBlank()) {
      throw new IllegalArgumentException("broker host must not be blank");
    }
    if (port <= 0 || port > 65535) {
      throw new IllegalArgumentException("broker port is out of range: " + port);
    }
  }

  /** 解析 host:port 格式的地址。 */
  public static BrokerEndpoint parse(String value) {
    Objects.requireNonNull(value, "endpoint value must not be null");
    if (value.contains("://")) {
      URI uri = URI.create(value);
      if (uri.getHost() == null || uri.getPort() <= 0) {
        throw new IllegalArgumentException("endpoint URI must include host and port: " + value);
      }
      return new BrokerEndpoint(uri.getHost(), uri.getPort());
    }

    int separator = value.lastIndexOf(':');
    if (separator <= 0 || separator == value.length() - 1) {
      throw new IllegalArgumentException("endpoint must use host:port format: " + value);
    }
    String host = value.substring(0, separator);
    int port = Integer.parseInt(value.substring(separator + 1));
    return new BrokerEndpoint(host, port);
  }

  /** 返回 host:port 格式。 */
  public String address() {
    return host + ":" + port;
  }

  @Override
  public String toString() {
    return address();
  }
}
