package com.geopulse.processing.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Suppresses duplicate pings using shared state in Redis.
 *
 * WHY Redis and not a HashSet: a partition can move to another instance during
 * a rebalance. In-memory dedup state doesn't move with it, so the new owner
 * would reprocess everything — which is EXACTLY the scenario dedup exists to
 * prevent. Dedup state must outlive partition ownership, so it must be shared.
 */
@Service
@RequiredArgsConstructor
public class DeduplicationService {

    private static final String KEY_PREFIX = "dedup:ping:";

    private final StringRedisTemplate redis;

    @Value("${geopulse.dedup.ttl-seconds:300}")
    private long ttlSeconds;

    /**
     * Atomically claim this ping. Returns true if WE are the first to see it
     * (process it), false if someone already claimed it (skip).
     *
     * setIfAbsent() maps to Redis `SET key val NX EX ttl` — a SINGLE atomic
     * operation that both CHECKS and CLAIMS. A separate exists()-then-set()
     * would race: two threads could both see "absent" and both proceed.
     *
     * The TTL is what makes this affordable. Without expiry we'd accumulate
     * 21.6 BILLION keys/day. At 5 minutes we hold ~75M keys (~5-7GB) — bounded.
     * The tradeoff, stated plainly: this window is NOT absolute. A duplicate
     * arriving 6 minutes late slips through. Sized to cover realistic
     * redelivery paths (rebalance = seconds, retries = ms) with margin.
     */
    public boolean claim(String driverId, long timestamp) {
        // The BUSINESS identity, not the transport identity. Kafka offsets
        // would catch rebalance duplicates but NOT a client that retried its
        // POST and produced two distinct Kafka messages. This key catches both.
        String key = KEY_PREFIX + driverId + ":" + timestamp;

        Boolean claimed = redis.opsForValue()
                .setIfAbsent(key, "1", Duration.ofSeconds(ttlSeconds));

        // Null means the Redis call itself failed. Fail OPEN (treat as claimed)
        // rather than closed: if Redis is down we'd rather process a possible
        // duplicate than drop every ping. Availability over perfect dedup —
        // consistent with every other choice in this system.
        return claimed == null || claimed;
    }
}