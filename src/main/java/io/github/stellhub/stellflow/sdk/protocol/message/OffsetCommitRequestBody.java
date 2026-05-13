package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.RequestBody;
import java.util.List;

/** OffsetCommit 请求体。 */
public record OffsetCommitRequestBody(String groupId, List<OffsetCommitTopic> topics)
    implements RequestBody {}
