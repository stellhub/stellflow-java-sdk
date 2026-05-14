package io.github.stellhub.stellflow.sdk.admin;

/** ListOffsets 查询位置。 */
public record OffsetSpec(long timestamp) {

  public static final long EARLIEST_TIMESTAMP = -2L;
  public static final long LATEST_TIMESTAMP = -1L;

  /** 查询最早 offset。 */
  public static OffsetSpec earliest() {
    return new OffsetSpec(EARLIEST_TIMESTAMP);
  }

  /** 查询最新 offset。 */
  public static OffsetSpec latest() {
    return new OffsetSpec(LATEST_TIMESTAMP);
  }

  /** 按时间戳查询 offset。 */
  public static OffsetSpec forTimestamp(long timestamp) {
    if (timestamp < 0) {
      throw new IllegalArgumentException("timestamp must be non-negative");
    }
    return new OffsetSpec(timestamp);
  }
}
