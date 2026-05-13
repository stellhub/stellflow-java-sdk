package io.github.stellhub.stellflow.sdk.client;

import java.time.Duration;

/** 简单重试策略。 */
public record RetryPolicy(int maxAttempts, Duration backoff) {

  public static RetryPolicy none() {
    return new RetryPolicy(1, Duration.ZERO);
  }

  public static RetryPolicy defaultPolicy() {
    return new RetryPolicy(3, Duration.ofMillis(100));
  }

  public RetryPolicy {
    if (maxAttempts <= 0) {
      throw new IllegalArgumentException("maxAttempts must be positive");
    }
    if (backoff == null || backoff.isNegative()) {
      throw new IllegalArgumentException("backoff must not be negative");
    }
  }
}
