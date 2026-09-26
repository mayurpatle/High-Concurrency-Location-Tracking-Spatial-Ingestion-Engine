package com.geopulse.processing.service;

import com.geopulse.common.spatial.HotCell;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;

/**
 * Finds cells carrying disproportionate load, and publishes the verdict to
 * Redis where the ingestion tier can read it.
 *
 * WHY IT READS FROM REDIS RATHER THAN RUNNING IN THE CONSUMER: a consumer sees
 * only its own partitions, so it cannot compute a GLOBAL median. But every
 * consumer already flushes counters to Redis additively (D-86), so Redis holds
 * the complete picture with no coordination. Commutative writes to a shared
 * store give you a global view for free.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class HotCellDetector {

    private static final String ACTIVITY_PREFIX = "cell:activity:";
    private static final String HOT_CELLS_KEY   = "hotcells:current";

    private final StringRedisTemplate redis;

    /** How far above the median counts as hot. Ratio, not absolute — 100/sec
     *  is nothing at peak and a crisis at 3am. */
    @Value("${geopulse.hotcell.median-multiplier:10.0}")
    private double medianMultiplier;

    /** The floor that stops us chasing noise. At 3am the median might be 2, so
     *  a perfectly ordinary cell would look 10x hot. EVERY ratio-based detector
     *  needs an absolute floor, or it goes haywire when the denominator shrinks. */
    @Value("${geopulse.hotcell.min-pings:500}")
    private long minPings;

    /** Salt width for a flagged cell. Kept small: each unit of salt is one more
     *  partition holding part of the cell's state (Part 3). */
    @Value("${geopulse.hotcell.salt:4}")
    private int salt;

    /**
     * Runs on a schedule, not per ping. Detection is cheap and slow-moving;
     * mitigation takes minutes to matter, so scanning every 30s is plenty.
     *
     * NOTE: with several instances this runs on ALL of them, each computing the
     * same verdict from the same data and writing the same result. Wasteful but
     * harmless — the write is idempotent. A ShedLock-style leader election is
     * the proper fix. TODO(Phase 7).
     */
    @Scheduled(fixedDelayString = "${geopulse.hotcell.scan-interval-ms:30000}")
    public void detect() {
        // KEYS is O(n) and blocks Redis — acceptable here because active cells
        // number in the hundreds and this runs every 30s, not per request.
        // At production scale this becomes SCAN with a cursor.
        // TODO(Phase 6): switch to SCAN and measure.
        Set<String> keys = redis.keys(ACTIVITY_PREFIX + "*");
        if (keys == null || keys.size() < 3) {
            return;   // too few cells for a median to mean anything
        }

        // Read all counts in one round trip.
        List<String> keyList = new ArrayList<>(keys);
        List<String> values = redis.opsForValue().multiGet(keyList);
        if (values == null) {
            return;
        }

        Map<String, Long> counts = new HashMap<>();
        for (int i = 0; i < keyList.size(); i++) {
            if (values.get(i) == null) continue;   // expired between KEYS and MGET
            counts.put(keyList.get(i).substring(ACTIVITY_PREFIX.length()),
                    Long.parseLong(values.get(i)));
        }
        if (counts.size() < 3) {
            return;
        }

        // MEDIAN, not mean. A mean is dragged up by the very outliers we're
        // hunting — one cell at 125/sec inflates the average and makes the
        // airport look LESS anomalous than it is. Median is robust to outliers,
        // which is exactly why it's the right baseline for outlier detection.
        List<Long> sorted = new ArrayList<>(counts.values());
        Collections.sort(sorted);
        double median = sorted.get(sorted.size() / 2);
        if (median <= 0) {
            return;
        }

        List<HotCell> hot = new ArrayList<>();
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            double ratio = e.getValue() / median;
            // BOTH conditions: far above typical AND genuinely busy.
            if (ratio >= medianMultiplier && e.getValue() >= minPings) {
                hot.add(new HotCell(e.getKey(), e.getValue(), ratio, salt));
            }
        }

        publish(hot);

        if (!hot.isEmpty()) {
            // Rare and operationally significant — worth logging, unlike
            // anything on the hot path.
            log.warn("Hot cells detected (median={}): {}", median,
                    hot.stream().map(h -> h.h3PartitionCell() + "@" + h.ratioToMedian() + "x").toList());
        }
    }

    /**
     * Publish as a Redis Set the ingestion tier polls.
     *
     * Why Redis rather than a Kafka topic: this is small, current-state data
     * that every ingestion pod needs the LATEST version of — not an event
     * stream anyone needs the history of. A compacted topic would also work;
     * Redis is simpler and we're already running it.
     *
     * The TTL is a safety valve: if this detector dies, the hot-cell set
     * EXPIRES and the system reverts to unsalted behaviour. Failing back to
     * "no special handling" is far safer than leaving stale salting applied
     * forever to a cell that went quiet.
     */
    private void publish(List<HotCell> hot) {
        try {
            if (hot.isEmpty()) {
                redis.delete(HOT_CELLS_KEY);
                return;
            }
            // Rewrite atomically-ish: delete then add. A brief gap is fine —
            // ingestion falls back to unsalted for a few milliseconds.
            redis.delete(HOT_CELLS_KEY);
            redis.opsForSet().add(HOT_CELLS_KEY,
                    hot.stream().map(HotCell::h3PartitionCell).toArray(String[]::new));
            redis.expire(HOT_CELLS_KEY, Duration.ofMinutes(5));
        } catch (Exception ex) {
            // Derived data again (D-87): a failed publish means we miss a cycle
            // of mitigation, not that ingestion breaks.
            log.warn("Failed to publish hot cells", ex);
        }
    }
}