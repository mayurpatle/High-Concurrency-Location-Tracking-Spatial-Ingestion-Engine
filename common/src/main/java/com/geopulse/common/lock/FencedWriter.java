package com.geopulse.common.lock;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Writes that reject a stale fencing token.
 *
 * ⭐ THIS is where the safety lives — not in the lock. A lock guarantees
 * mutual exclusion among processes that are CHECKING it; it cannot stop a
 * write already in flight from a holder that stalled past its own TTL.
 *
 *   T1  A acquires (token 33)      T5  B acquires (token 34)
 *   T3  A pauses 40s (GC)          T6  B writes 34 → accepted
 *   T4  A's lock expires           T7  A writes 33 → REJECTED
 *
 * Two processes both believed they held the lock. Only one write survives,
 * and deterministically the newer one.
 *
 * ⭐ Note what this does NOT do: it doesn't prevent the race. It makes the
 * race's OUTCOME correct — the same pattern as last-write-wins on device
 * timestamps (D-37), idempotent Cassandra keys (D-63), and additive counter
 * flushes (D-86).
 */
@Service
@RequiredArgsConstructor
public class FencedWriter {

    private final StringRedisTemplate redis;

    /**
     * Check-and-write, atomically. GET-then-SET would be the very TOCTOU race
     * we're closing, so it has to be a script.
     */
    private static final DefaultRedisScript<Long> FENCED_WRITE = new DefaultRedisScript<>("""
            -- KEYS[1] resource, ARGV[1] token, ARGV[2] value
            local stored = redis.call('HGET', KEYS[1], 'fence')
            if stored and tonumber(stored) >= tonumber(ARGV[1]) then
                return 0    -- stale (or replayed) holder: reject
            end
            redis.call('HSET', KEYS[1], 'fence', ARGV[1], 'value', ARGV[2])
            return 1
            """, Long.class);

    /**
     * @return true if written, false if a newer token had already been seen.
     *
     * ⚠️ A `false` here is not an error to retry — it means another holder
     * legitimately superseded you. Retrying would be wrong: reacquire the lock
     * and re-evaluate, because the state you based your decision on has changed.
     */
    public boolean write(String resource, FencedLock lock, String value) {
        Long ok = redis.execute(FENCED_WRITE,
                List.of("fenced:" + resource),
                String.valueOf(lock.fenceToken()), value);
        return ok != null && ok > 0;
    }
}