package io.github.stellhub.stellflow.sdk.producer;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Round-robin 分区器。 */
public class RoundRobinProducerPartitioner implements ProducerPartitioner {

    private final ConcurrentMap<String, AtomicInteger> cursors = new ConcurrentHashMap<>();

    @Override
    public int partition(String topic, byte[] key, byte[] value, List<Integer> partitions) {
        Objects.requireNonNull(topic, "topic must not be null");
        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalArgumentException("partitions must not be empty");
        }
        int index =
                Math.floorMod(
                        cursors.computeIfAbsent(topic, ignored -> new AtomicInteger()).getAndIncrement(),
                        partitions.size());
        return partitions.get(index);
    }
}
