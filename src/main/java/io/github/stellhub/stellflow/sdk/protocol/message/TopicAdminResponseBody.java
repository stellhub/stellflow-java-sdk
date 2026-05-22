package io.github.stellhub.stellflow.sdk.protocol.message;

import io.github.stellhub.stellflow.sdk.protocol.ResponseBody;
import java.util.List;

/** Topic 管理响应体。 */
public record TopicAdminResponseBody(String topic, List<TopicAdminPartitionResponse> partitions)
        implements ResponseBody {

    public TopicAdminResponseBody {
        partitions = List.copyOf(partitions);
    }
}
