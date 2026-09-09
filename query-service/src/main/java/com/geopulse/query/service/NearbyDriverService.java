package com.geopulse.query.service;

import com.geopulse.common.dto.NearbyDriver;
import com.geopulse.common.spatial.H3IndexService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * "Which drivers are near this point, right now?"
 *
 * The read side of the H3 index built in Session 3.1. Two pipelined round
 * trips regardless of radius:
 *   1. ZRANGEBYSCORE across all cells in the k-ring  -> candidate driver IDs
 *   2. HMGET each candidate's position               -> filter by true distance
 *
 * Whether that's 19 cells or 169, it's still two trips. That's the payoff of
 * pipelining, and the reason res 9 was chosen (169 cells for a 2km search).
 */
@Service
@RequiredArgsConstructor
public class NearbyDriverService {

    private static final String CELL_KEY_PREFIX   = "cell:";
    private static final String DRIVER_KEY_PREFIX = "driver:";

    private final StringRedisTemplate redis;
    private final H3IndexService h3IndexService;

    @Value("${geopulse.redis.presence-ttl-seconds:30}")
    private long presenceTtlSeconds;

    public List<NearbyDriver> findNearby(double lat, double lng, double radiusMetres, int limit) {

        // ---- 1. Expand the search area into cells ----
        String originCell = h3IndexService.toStorageCell(lat, lng);
        int k = h3IndexService.ringsForRadius(radiusMetres);
        List<String> cells = h3IndexService.neighbours(originCell, k);

        // ---- 2. Fetch candidate driver IDs from every cell, in ONE trip ----
        // ZRANGEBYSCORE with a freshness floor, NOT ZRANGE: stale members are
        // filtered AT READ TIME. A driver who stopped pinging 5 minutes ago
        // never appears, even though the opportunistic sweep hasn't removed
        // them yet. This is why reads stay correct while cleanup is only
        // housekeeping (Session 3.1).
        long freshnessFloor = System.currentTimeMillis() - (presenceTtlSeconds * 1000);

        List<Object> cellResults = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (String cell : cells) {
                connection.zSetCommands().zRangeByScore(
                        (CELL_KEY_PREFIX + cell).getBytes(),
                        freshnessFloor,
                        Double.POSITIVE_INFINITY);
            }
            return null;
        });

        // Union the results. A Set because a driver could theoretically appear
        // in two cells during a move (the SREM and ZADD aren't atomic across
        // pipeline boundaries) — dedupe defensively.
        Set<String> candidateIds = new LinkedHashSet<>();
        for (Object result : cellResults) {
            if (result instanceof Set<?> members) {
                for (Object member : members) {
                    candidateIds.add((String)  member);
                }
            }
        }

        if (candidateIds.isEmpty()) {
            return List.of();
        }

        // ---- 3. Fetch each candidate's position, in ONE trip ----
        // HMGET only the fields we need. At scale, pulling speed/accuracy we
        // won't use is wasted bandwidth on every query.
        List<String> ids = new ArrayList<>(candidateIds);
        List<Object> positionResults = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (String id : ids) {
                connection.hashCommands().hMGet(
                        (DRIVER_KEY_PREFIX + id + ":loc").getBytes(),
                        "lat".getBytes(), "lng".getBytes(),
                        "ts".getBytes(), "heading".getBytes());
            }
            return null;
        });

        // ---- 4. Filter by TRUE distance, because k-rings over-include ----
        List<NearbyDriver> nearby = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            @SuppressWarnings("unchecked")
            List<String> fields = (List<String>) positionResults.get(i);   // String, not byte[]

            // Null means the driver hash expired between the two round trips —
            // a genuine race, not an error. Skip them.
            if (fields == null || fields.get(0) == null || fields.get(1) == null) {
                continue;
            }

            double dLat = Double.parseDouble(fields.get(0));
            double dLng = Double.parseDouble(fields.get(1));

            double distance = h3IndexService.distanceMetres(lat, lng, dLat, dLng);
            if (distance > radiusMetres) {
                continue;
            }

            long ts = fields.get(2) == null ? 0L : Long.parseLong(fields.get(2));
            Double heading = fields.get(3) == null ? null : Double.parseDouble(fields.get(3));

            nearby.add(new NearbyDriver(ids.get(i), dLat, dLng, distance, ts, heading));
        }

        // ---- 5. Nearest first, capped ----
        // Sorting in Java, not Redis: we're sorting by a distance Redis doesn't
        // know about (it stores timestamps as scores, not distances).
        nearby.sort(Comparator.comparingDouble(NearbyDriver::distanceMetres));
        return nearby.size() > limit ? nearby.subList(0, limit) : nearby;
    }

    /**
     * Driver count in a single cell — O(1), the supply-density signal.
     *
     * This is what Project 3's surge pricing reads. Note how much cheaper it is
     * than the equivalent with a GEO index, where you'd have to run a radius
     * search and count the results. The CELL is a unit of aggregation, not
     * just a search index — that's a big part of why we chose H3 sets.
     */
    public long countInCell(String h3Cell) {
        long freshnessFloor = System.currentTimeMillis() - (presenceTtlSeconds * 1000);
        Long count = redis.opsForZSet().count(
                CELL_KEY_PREFIX + h3Cell, freshnessFloor, Double.POSITIVE_INFINITY);
        return count == null ? 0L : count;
    }
}