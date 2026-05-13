package io.github.stellhub.stellflow.sdk.protocol.message;

import java.util.List;

/** 消息记录。 */
public record StellflowRecord(
    byte attributes,
    long timestampDelta,
    int offsetDelta,
    byte[] key,
    byte[] value,
    List<StellflowRecordHeader> headers) {}
