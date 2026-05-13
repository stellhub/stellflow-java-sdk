package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import java.util.List;

/** Topic 元数据响应。 */
public record MetadataTopicResponse(
    ErrorCode errorCode,
    String topic,
    boolean internal,
    List<MetadataPartitionResponse> partitions,
    int topicAuthorizedOperations) {}
