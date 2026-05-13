package io.github.stellhub.stellflow.sdk.consumer;

import java.time.Duration;
import java.util.Objects;

/** Consumer 高层行为配置。 */
public record StellflowConsumerOptions(
    String groupId,
    String memberId,
    int sessionTimeoutMs,
    Duration heartbeatInterval,
    int fetchMaxBytes,
    String offsetCommitMetadata) {

  public static final int DEFAULT_SESSION_TIMEOUT_MS = 30_000;
  public static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(3);
  public static final int DEFAULT_FETCH_MAX_BYTES = 1024 * 1024;

  /** 创建默认 Consumer 配置。 */
  public static StellflowConsumerOptions defaults(String groupId) {
    return new StellflowConsumerOptions(
        groupId,
        "",
        DEFAULT_SESSION_TIMEOUT_MS,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_FETCH_MAX_BYTES,
        "");
  }

  public StellflowConsumerOptions {
    if (groupId == null || groupId.isBlank()) {
      throw new IllegalArgumentException("groupId must not be blank");
    }
    memberId = memberId == null ? "" : memberId;
    if (sessionTimeoutMs <= 0) {
      throw new IllegalArgumentException("sessionTimeoutMs must be positive");
    }
    Objects.requireNonNull(heartbeatInterval, "heartbeatInterval must not be null");
    if (heartbeatInterval.isZero() || heartbeatInterval.isNegative()) {
      throw new IllegalArgumentException("heartbeatInterval must be positive");
    }
    if (fetchMaxBytes <= 0) {
      throw new IllegalArgumentException("fetchMaxBytes must be positive");
    }
    offsetCommitMetadata = offsetCommitMetadata == null ? "" : offsetCommitMetadata;
  }
}
