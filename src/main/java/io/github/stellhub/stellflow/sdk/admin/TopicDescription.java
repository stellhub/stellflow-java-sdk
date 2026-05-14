package io.github.stellhub.stellflow.sdk.admin;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;
import java.util.List;

/** Topic 描述结果。 */
public record TopicDescription(
    String topic,
    ErrorCode errorCode,
    boolean internal,
    List<PartitionDescription> partitions,
    int topicAuthorizedOperations) {

  public TopicDescription {
    partitions = List.copyOf(partitions);
  }
}
