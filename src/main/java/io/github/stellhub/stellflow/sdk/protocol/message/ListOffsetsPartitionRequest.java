package io.github.stellhub.stellflow.sdk.protocol.message;

/** ListOffsets 分区请求。 */
public record ListOffsetsPartitionRequest(
        int partition, int currentLeaderEpoch, long timestamp, int maxNumOffsets) {}
