package com.geopulse.common.lock;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * A distributed lock with fencing tokens.
 *
 * ⚠️ NO CALLER IN PROJECT 1. Every case here was solved without a lock:
 * partition affinity for the move logic, atomic SET NX for dedup, idempotent
 * primary keys for history, commutative merges for counters. This exists for
 * Project 2's driver-assignment invariant, where one constraint spans two
 * entities that cannot be co-partitioned (D-88).
 *
 * ⚠️ SINGLE-INSTANCE REDIS, DELIBERATELY. Not Redlock. Kleppmann's argument:
 * if you have fencing tokens you don't need Redlock (the token catches the
 * failure anyway); if you don't, Redlock doesn't save you (it can still grant
 * concurrent access under a GC pause). Either way the 5-instance complexity
 * buys nothing. The safety here comes from the TOKEN, not the lock.
 *
 * ⚠️ THE DURABILITY CAVEAT: token monotonicity is only as good as the
 * counter's durability. Redis replication is ASYNCHRONOUS, so a promoted
 * replica that missed the last few INCRs will reissue tokens it already gave
 * out — and a repeated token defeats fencing entirely. For a true correctness
 * lock, source tokens from a consensus system (ZooKeeper zxid, etcd revision)
 * or a durable DB sequence. TODO(Project 2): decide based on how much a double
 * assignment actually costs.
 */
@Service
@RequiredArgsConstructor
public class DistributedLockService {

    private static final String LOCK_PREFIX  = "lock:";
    private static final String FENCE_PREFIX = "fence:";

    private final StringRedisTemplate redis;

    /**
     * Acquire-and-mint, atomically.
     *
     * Must be one script: SET NX then INCR would leave a window where we hold
     * the lock but have no token, and a crash there would strand the lock.
     */
    private static final DefaultRedisScript<Long> ACQUIRE = new DefaultRedisScript<>("""
            -- KEYS[1] lock key, KEYS[2] fence counter
            -- ARGV[1] owner id, ARGV[2] ttl millis
            if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
                -- Mint a token ONLY on successful acquisition, so tokens
                -- strictly increase with each granted lock.
                return redis.call('INCR', KEYS[2])
            else
                return -1
            end
            """, Long.class);

    /**
     * Release only if WE still hold it.
     *
     * A bare DEL is the classic bug: if our lock expired and someone else
     * acquired it, DEL removes THEIRS. The compare must be atomic with the
     * delete, so GET-then-DEL is exactly the race we're preventing.
     */
    private static final DefaultRedisScript<Long> RELEASE = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    public Optional<FencedLock> tryAcquire(String resource, Duration ttl) {
        String ownerId = UUID.randomUUID().toString();

        Long token = redis.execute(ACQUIRE,
                List.of(LOCK_PREFIX + resource, FENCE_PREFIX + resource),
                ownerId, String.valueOf(ttl.toMillis()));

        if (token == null || token < 0) {
            return Optional.empty();   // held by someone else
        }
        return Optional.of(new FencedLock(resource, ownerId, token));
    }

    /**
     * Best-effort release. Returning false is NOT an error — it means the TTL
     * already expired and possibly someone else holds it now.
     *
     * ⚠️ If it returns false, assume your work may have raced. That's precisely
     * what the fencing token protects against, and why you should never treat
     * a successful release as proof your write landed.
     */
    public boolean release(FencedLock lock) {
        Long released = redis.execute(RELEASE,
                List.of(LOCK_PREFIX + lock.resource()), lock.ownerId());
        return released != null && released > 0;
    }
}