package com.geopulse.processing.service;

import com.geopulse.common.model.LocationPing;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes current driver state to Redis — the "right now" tier.
 *
 * Takes a LIST, never a single ping. A per-ping signature would mean 500
 * round trips per batch; batched it's ~3. You pay for the network TRIP,
 * not the payload.
 *
 * CONCURRENCY: this performs an unguarded read-modify-write on shared state
 * (read previous cell -> move driver between cell indexes). That is safe
 * WITHOUT a lock because pings are keyed by H3 res-7 cell, so one Kafka
 * partition -> one consumer thread owns every driver in a region. This is
 * "the partition is your lock" doing actual work.
 */
@Service
@RequiredArgsConstructor
public class RedisLocationWriter {

    private static final String DRIVER_KEY_PREFIX = "driver:";
    private static final String CELL_KEY_PREFIX   = "cell:";

    private static final String FIELD_LAT     = "lat";
    private static final String FIELD_LNG     = "lng";
    private static final String FIELD_TS      = "ts";
    private static final String FIELD_H3      = "h3";
    private static final String FIELD_SPEED   = "speed";
    private static final String FIELD_HEADING = "heading";

    private final StringRedisTemplate redis;

    @Value("${geopulse.redis.presence-ttl-seconds:30}")
    private long presenceTtlSeconds;

    public void writeAll(List<LocationPing> pings) {
        if (pings.isEmpty()) {
            return;
        }

        // ---- STEP 1: read each driver's CURRENT stored state (one round trip) ----
        // We need two things per driver:
        //   - the previous h3 cell, so we can remove them from it if they moved
        //   - the stored timestamp, so we can reject a STALE ping
        //
        // Why a separate round trip: a pipeline sends all commands then collects
        // all replies — you cannot branch on a reply mid-pipeline. So: read
        // everything, decide in Java, then write everything.
        Map<String, PreviousState> previous = readPreviousState(pings);

        // ---- STEP 2: write everything in ONE pipeline ----
        redis.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) connection -> {

            long cutoffMillis = System.currentTimeMillis() - (presenceTtlSeconds * 1000);

            for (LocationPing ping : pings) {
                PreviousState prev = previous.get(ping.driverId());

                // ---- STALE-WRITE GUARD (last-write-wins by device timestamp) ----
                // The boundary race from Session 1.3: a driver crossing between
                // cells on DIFFERENT partitions has no ordering guarantee, so an
                // older ping can arrive after a newer one. Without this check,
                // the driver's position would jump BACKWARDS.
                //
                // Note >= : an equal timestamp is a duplicate that slipped past
                // dedup (e.g. after the TTL window). Rewriting it is harmless but
                // pointless, so we skip.
                if (prev != null && prev.timestamp >= ping.timestamp()) {
                    continue;
                }

                String driverKey = DRIVER_KEY_PREFIX + ping.driverId() + ":loc";
                String newCellKey = CELL_KEY_PREFIX + ping.h3Cell();

                // ---- A. Move between cell indexes, if the cell changed ----
                // Without this, a driver accumulates membership in every cell
                // they have EVER visited — ghost drivers that a proximity search
                // would happily return.
                if (prev != null && prev.h3Cell != null && !prev.h3Cell.equals(ping.h3Cell())) {
                    connection.setCommands().sRem(
                            (CELL_KEY_PREFIX + prev.h3Cell).getBytes(),
                            ping.driverId().getBytes());
                }

                // ---- B. Add/refresh membership in the current cell ----
                // Sorted Set with the DEVICE TIMESTAMP as score. Two wins:
                //   - per-member freshness (a whole-key TTL would evict drivers
                //     who are still actively pinging)
                //   - reads can filter stale members immediately via
                //     ZRANGEBYSCORE, before any sweep runs
                // ZADD on an existing member just updates the score — which is
                // exactly the "refresh presence" semantic we want, and makes
                // this idempotent under at-least-once redelivery.
                connection.zSetCommands().zAdd(
                        newCellKey.getBytes(),
                        ping.timestamp(),
                        ping.driverId().getBytes());

                // ---- C. Opportunistic sweep of stale members in this cell ----
                // Evict anyone whose last ping is older than the presence window.
                // Cheap (O(log n + m), and m is usually 0) and it runs naturally
                // wherever there's traffic — no cron, no separate cleanup job.
                //
                // TODO(Phase 5): this sweeps only cells we happen to write to.
                // A cell that goes COMPLETELY silent is never swept and keeps its
                // stale members. Queries filter by score so reads stay correct,
                // but the memory leaks. Needs a background sweeper or key TTL.
                connection.zSetCommands().zRemRangeByScore(
                        newCellKey.getBytes(), 0, cutoffMillis);

                // ---- D. The driver's current position ----
                // A Hash, not a JSON String: Redis stores small hashes in a
                // compact listpack encoding, which is meaningfully more
                // memory-efficient at a million drivers — and avoids a
                // serialization round trip on read.
                Map<byte[], byte[]> fields = new HashMap<>();
                fields.put(FIELD_LAT.getBytes(),  String.valueOf(ping.lat()).getBytes());
                fields.put(FIELD_LNG.getBytes(),  String.valueOf(ping.lng()).getBytes());
                fields.put(FIELD_TS.getBytes(),   String.valueOf(ping.timestamp()).getBytes());
                fields.put(FIELD_H3.getBytes(),   ping.h3Cell().getBytes());
                // Optional fields: only write when present. Writing "null" as a
                // string would be worse than absent — a reader can't distinguish
                // "not reported" from "the literal text null".
                if (ping.speed() != null) {
                    fields.put(FIELD_SPEED.getBytes(), String.valueOf(ping.speed()).getBytes());
                }
                if (ping.heading() != null) {
                    fields.put(FIELD_HEADING.getBytes(), String.valueOf(ping.heading()).getBytes());
                }
                connection.hashCommands().hMSet(driverKey.getBytes(), fields);

                // TTL refreshed on every ping. Here a whole-key TTL is CORRECT,
                // because the key holds exactly one driver.
                // Key exists -> pinged recently -> online. Key gone -> dark.
                // Presence for free: no "offline" flag, no tombstones, no cleanup.
                connection.keyCommands().expire(
                        driverKey.getBytes(), presenceTtlSeconds);
            }
            return null;   // pipelined replies are collected but we don't need them
        });
    }

    /**
     * Fetch each driver's stored cell + timestamp in ONE pipelined round trip.
     */
    private Map<String, PreviousState> readPreviousState(List<LocationPing> pings) {
        List<Object> results = redis.executePipelined(
                (org.springframework.data.redis.core.RedisCallback<Object>) connection -> {
                    for (LocationPing ping : pings) {
                        // HMGET just the two fields we need — not HGETALL. At
                        // 250k/s, transferring speed/heading/lat/lng we're about
                        // to overwrite is pure waste.
                        connection.hashCommands().hMGet(
                                (DRIVER_KEY_PREFIX + ping.driverId() + ":loc").getBytes(),
                                FIELD_H3.getBytes(), FIELD_TS.getBytes());
                    }
                    return null;
                });

        Map<String, PreviousState> previous = new HashMap<>();
        for (int i = 0; i < pings.size(); i++) {
            @SuppressWarnings("unchecked")
            List<byte[]> fields = (List<byte[]>) results.get(i);

            // Both null => the driver has no stored state (new, or expired).
            if (fields == null || fields.get(0) == null) {
                continue;
            }
            String cell = new String(fields.get(0));
            long ts = fields.get(1) == null ? 0L : Long.parseLong(new String(fields.get(1)));
            previous.put(pings.get(i).driverId(), new PreviousState(cell, ts));
        }
        return previous;
    }

    /** The two fields we need from a driver's stored state. */
    private record PreviousState(String h3Cell, long timestamp) {}
}