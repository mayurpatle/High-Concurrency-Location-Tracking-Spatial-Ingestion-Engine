package com.geopulse.ingestion.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A locally-cached view of which cells are currently hot.
 *
 * WHY CACHED LOCALLY: this is consulted on EVERY ping (250k/sec). A Redis
 * lookup per ping would be a round trip on the hot path — exactly the blocking
 * I/O this architecture exists to avoid. We poll every few seconds instead and
 * read from a volatile field.
 *
 * Staleness is fine: a cell that just went hot is unsalted for a few more
 * seconds, and a cell that cooled stays salted a little longer. Neither is
 * harmful — mitigation is a load optimization, not a correctness mechanism.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class HotCellRegistry {

    private static final String HOT_CELLS_KEY = "hotcells:current";

    private final StringRedisTemplate redis;

    @Value("${geopulse.hotcell.salt:4}")
    private int saltWidth;

    /**
     * volatile: written by the scheduler thread, read by every request thread.
     * We REPLACE the reference rather than mutating a shared set — readers
     * always see a complete, consistent snapshot and never a half-updated one.
     * Cheaper and safer than locking around a mutable collection.
     */
    private volatile Set<String> hotCells = Set.of();

    @Scheduled(fixedDelayString = "${geopulse.hotcell.refresh-ms:5000}")
    public void refresh() {
        try {
            Set<String> latest = redis.opsForSet().members(HOT_CELLS_KEY);
            Set<String> updated = (latest == null) ? Set.of() : Set.copyOf(latest);
            if (!updated.equals(hotCells)) {
                log.info("Hot cell set changed: {} -> {}", hotCells, updated);
            }
            hotCells = updated;
        } catch (Exception e) {
            // FAIL TOWARD NORMAL BEHAVIOUR: keep the last known set rather than
            // clearing it. If Redis is down we'd rather keep salting a cell
            // that was hot 10 seconds ago than suddenly dump its full load onto
            // one partition at the worst possible moment.
            log.warn("Failed to refresh hot cells; keeping previous set", e);
        }
    }

    /**
     * The routing key for a ping. Salted only if the cell is flagged.
     *
     * Called 250,000 times a second — a Set lookup and, rarely, a string
     * concat. Nothing else belongs here.
     */
    public String routingKey(String partitionCell) {
        if (!hotCells.contains(partitionCell)) {
            return partitionCell;
        }
        // RANDOM, not round-robin: round-robin needs a shared counter across
        // ingestion pods, and that's coordination on the hot path. Random is
        // statistically even over a large number of messages and costs nothing.
        return partitionCell + "#" + ThreadLocalRandom.current().nextInt(saltWidth);
    }
}