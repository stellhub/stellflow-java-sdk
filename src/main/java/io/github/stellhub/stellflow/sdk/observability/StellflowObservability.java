package io.github.stellhub.stellflow.sdk.observability;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Objects;

/** Stellflow 客户端可观测性入口。 */
public final class StellflowObservability {

  public static final String INSTRUMENTATION_SCOPE = "io.github.stellhub.stellflow.sdk";

  private static final StellflowObservability GLOBAL =
      new StellflowObservability(GlobalOpenTelemetry.getOrNoop());

  private final OpenTelemetry openTelemetry;
  private final StellflowClientMetrics metrics;

  public StellflowObservability(OpenTelemetry openTelemetry) {
    this.openTelemetry = Objects.requireNonNull(openTelemetry, "openTelemetry must not be null");
    this.metrics = new StellflowClientMetrics(openTelemetry);
  }

  /** 返回全局 OpenTelemetry 绑定。 */
  public static StellflowObservability global() {
    return GLOBAL;
  }

  /** 使用指定 OpenTelemetry 创建观测入口。 */
  public static StellflowObservability create(OpenTelemetry openTelemetry) {
    return new StellflowObservability(openTelemetry);
  }

  /** 返回 OpenTelemetry 实例。 */
  public OpenTelemetry openTelemetry() {
    return openTelemetry;
  }

  /** 返回指标记录器。 */
  public StellflowClientMetrics metrics() {
    return metrics;
  }
}
