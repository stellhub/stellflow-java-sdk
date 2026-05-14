package io.github.stellhub.stellflow.sdk.producer;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/** 默认分区器：key hash 优先，无 key 时 round-robin。 */
public class DefaultProducerPartitioner implements ProducerPartitioner {

    private final ConcurrentMap<String, AtomicInteger> cursors = new ConcurrentHashMap<>();

    @Override
    public int partition(String topic, byte[] key, byte[] value, List<Integer> partitions) {
        Objects.requireNonNull(topic, "topic must not be null");
        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalArgumentException("partitions must not be empty");
        }
        if (key != null && key.length > 0) {
            int index = Math.floorMod(Arrays.hashCode(key), partitions.size());
            return partitions.get(index);
        }
        int index =
                Math.floorMod(
                        cursors.computeIfAbsent(topic, ignored -> new AtomicInteger()).getAndIncrement(),
                        partitions.size());
        return partitions.get(index);
    }
}
