package io.github.stellhub.stellflow.sdk.consumer;

import java.util.List;

/** Consumer 订阅 payload。 */
public record ConsumerSubscriptionPayload(String memberId, List<String> topics) {

    public ConsumerSubscriptionPayload {
        memberId = memberId == null ? "" : memberId;
        topics = List.copyOf(topics == null ? List.of() : topics);
    }
}
