package com.geopulse.processing.service;

import com.geopulse.common.model.LocationPing;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Suppresses duplicate pings using shared state in Redis.
 *
 * BATCHED as of Phase 6.3. The per-record version did one round trip per
 * record — 500 × ~0.3ms = ~150ms of SEQUENTIAL latency per batch, before the
 * batched write even started. That was ~83% of measured Redis write latency.
 *
 * Still Redis rather than a HashMap: a partition can move to another instance
 * during a rebalance, and in-memory dedup state doesn't move with it — so it
 * would fail exactly when you need it (D-24).
 */
@Service
@RequiredArgsConstructor
public class DeduplicationService {

    private static final String KEY_PREFIX = "dedup:ping:";

    private final StringRedisTemplate redis;

    @Value("${geopulse.dedup.ttl-seconds:300}")
    private long ttlSeconds;

    /**
     * Claim a whole batch in ONE round trip.
     *
     * @return a parallel list: true at index i means WE claimed pings[i] and
     *         should process it; false means someone already did.
     *
     * The ordering contract matters — executePipelined returns results in the
     * order commands were queued, so index i of the result corresponds to
     * index i of the input. Correlating by position is what makes this work
     * without sending the key back and forth.
     */
    public List<Boolean> claimAll(List<LocationPing> pings) {
        if (pings.isEmpty()) {
            return List.of();
        }

        List<Object> results = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (LocationPing ping : pings) {
                // SET key "1" NX EX ttl — still one atomic check-and-claim per
                // ping, just no longer one ROUND TRIP per ping. The atomicity
                // is per-command and unaffected by pipelining; only the network
                // cost is amortized.
                connection.stringCommands().set(
                        key(ping).getBytes(),
                        "1".getBytes(),
                        org.springframework.data.redis.core.types.Expiration.seconds(ttlSeconds),
                        org.springframework.data.redis.connection.RedisStringCommands.SetOption.ifAbsent());
            }
            return null;
        });

        List<Boolean> claimed = new ArrayList<>(pings.size());
        for (int i = 0; i < pings.size(); i++) {
            Object r = (i < results.size()) ? results.get(i) : null;

            // SET NX returns true when it set the key (we claimed it), false
            // when the key already existed.
            //
            // A NULL means the command itself failed. FAIL OPEN — treat it as
            // claimed and process the ping. If Redis is misbehaving we'd rather
            // process a possible duplicate than drop data. A duplicate is a
            // minor data-quality issue; dropping every ping is an outage (D-26).
            claimed.add(r == null || Boolean.TRUE.equals(r));
        }
        return claimed;
    }

    private String key(LocationPing ping) {
        // The BUSINESS identity, not the transport identity. Kafka offsets
        // would catch rebalance duplicates but not a client that retried its
        // POST and produced two distinct Kafka messages.
        return KEY_PREFIX + ping.driverId() + ":" + ping.timestamp();
    }
}