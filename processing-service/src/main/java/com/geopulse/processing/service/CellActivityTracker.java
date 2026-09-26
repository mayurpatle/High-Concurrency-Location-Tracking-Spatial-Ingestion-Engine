package com.geopulse.processing.service;

import com.geopulse.common.model.LocationPing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-cell activity counters, accumulated IN MEMORY and flushed periodically.
 *
 * WHY IN MEMORY: at 250k pings/sec, a Redis increment per ping is 250,000 extra
 * round trips per second — a second write path as costly as the first. The
 * answer changes slowly and only needs to be accurate to a few seconds, so we
 * accumulate locally and flush: ONE write per cell per interval instead of one
 * per ping (~99% fewer).
 *
 * WHY NO SYNCHRONIZATION: the partition guarantee. A res-7 cell maps to exactly
 * one partition, owned by exactly one thread, so the thread that accumulates a
 * cell's count is the only thread that will ever touch it. This class is the
 * guarantee cashing out — without it, a ConcurrentHashMap with atomic merges at
 * best, a distributed counter at worst.
 *
 * WHY KEYED BY PARTITION: counters must be flushable when a partition is
 * REVOKED. The guarantee gives exclusion at an instant, not ownership over
 * time — see flushPartition().
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CellActivityTracker {

    private static final String ACTIVITY_KEY_PREFIX = "cell:activity:";

    private final StringRedisTemplate redis;

    @Value("${geopulse.activity.ttl-seconds:900}")
    private long activityTtlSeconds;

    /**
     * partition -> (cell -> ping count since last flush).
     *
     * The OUTER map is ConcurrentHashMap because several listener threads read
     * and write different partitions concurrently, and the rebalance listener
     * runs on the consumer thread being revoked.
     *
     * The INNER maps are plain HashMaps — deliberately. Each is only ever
     * touched by the single thread owning that partition. Using a concurrent
     * map there would pay for a guarantee we already have structurally.
     */
    private final Map<Integer, Map<String, Long>> countsByPartition = new ConcurrentHashMap<>();

    /**
     * Record one ping. Pure in-memory: no I/O, no lock, no allocation in the
     * common case. This runs 250,000 times a second, so it must stay trivial.
     */
    public void record(int partition, LocationPing ping) {
        countsByPartition
                .computeIfAbsent(partition, p -> new HashMap<>())
                .merge(ping.h3PartitionCell(), 1L, Long::sum);
    }

    /**
     * Flush one partition's counters and clear them.
     *
     * Called after each batch (normal path) AND on partition revocation
     * (lifecycle path). Idempotent by way of remove(): whoever gets the map
     * first owns flushing it.
     */
    public void flushPartition(int partition) {
        Map<String, Long> counts = countsByPartition.remove(partition);
        if (counts == null || counts.isEmpty()) {
            return;
        }

        try {
            redis.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
                for (Map.Entry<String, Long> e : counts.entrySet()) {
                    byte[] key = (ACTIVITY_KEY_PREFIX + e.getKey()).getBytes();

                    // INCRBY, not SET — the flush must be ADDITIVE.
                    // A dying owner's partial flush and the new owner's fresh
                    // count then ADD UP instead of clobbering each other. With
                    // SET, a new owner starting from zero would overwrite a
                    // larger flushed value and the count would go BACKWARDS.
                    conn.stringCommands().incrBy(key, e.getValue());

                    // Refreshed on every flush: a cell that goes quiet ages out
                    // rather than holding a stale count forever.
                    conn.keyCommands().expire(key, activityTtlSeconds);
                }
                return null;
            });
        } catch (Exception ex) {
            // Do NOT rethrow. These counters are a derived signal, not the
            // system of record — losing an interval's worth is a small gap in
            // a metric. Rethrowing would fail the batch and block the offset
            // commit, turning a metrics problem into an INGESTION problem.
            // Never let derived data take down the primary path.
            log.warn("Failed to flush activity counters for partition {}", partition, ex);
        }
    }
}