package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;
import java.util.List;

/** OffsetCommit 响应体。 */
public record OffsetCommitResponseBody(List<OffsetCommitTopicResponse> topics)
        implements ResponseBody {}
