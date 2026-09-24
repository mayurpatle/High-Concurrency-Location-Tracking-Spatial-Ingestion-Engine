package com.geopulse.query.controller;

import com.geopulse.common.dto.NearbyDriver;
import com.geopulse.common.dto.NearbyResponse;
import com.geopulse.common.dto.OccupancyResponse;
import com.geopulse.common.dto.TrajectoryResponse;
import com.geopulse.common.spatial.H3IndexService;
import com.geopulse.query.exception.SpatialUnavailableException;
import com.geopulse.query.service.NearbyDriverService;
import com.geopulse.query.service.OccupancyService;
import com.geopulse.query.service.TrajectoryService;
import jakarta.validation.constraints.*;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * The read side. Serves "right now" questions from Redis.
 *
 * Note what's absent: no Kafka, no Cassandra (yet), no writes. This service
 * scales independently of ingestion — reads are bursty and user-driven while
 * writes are a relentless metronome, so they belong on separate deployments.
 */
@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
@Validated   // enables validation of @RequestParam constraints below
public class DriverQueryController {

    private final NearbyDriverService nearbyDriverService;


    private  final TrajectoryService trajectoryService   ;

    private final OccupancyService occupancyService   ;


    private final StringRedisTemplate redis;
    private final H3IndexService h3IndexService;

    /**
     * GET /v1/drivers/nearby?lat=&lng=&radiusMetres=&limit=
     *
     * THE query this whole system exists to answer — and the primitive
     * Project 2's matching engine will consume.
     */
    @GetMapping("/drivers/nearby")
    public NearbyResponse nearby(
            @RequestParam @DecimalMin("-90.0")  @DecimalMax("90.0")  double lat,
            @RequestParam @DecimalMin("-180.0") @DecimalMax("180.0") double lng,

            // Capped at 10km. An uncapped radius is a DoS vector: 50km would be
            // k=167 rings = ~84,000 cells in one pipeline. Never let a client
            // choose an unbounded amount of work.
            @RequestParam(defaultValue = "2000")
            @Positive @Max(10000) double radiusMetres,

            @RequestParam(defaultValue = "50") @Positive @Max(500) int limit) {

        return nearbyDriverService.findNearby(lat, lng, radiusMetres, limit);
    }

    /**
     * GET /v1/drivers/{driverId}/location
     *
     * 404 when absent — and that is NOT a special case. We store no "offline"
     * flag; the TTL having fired IS the offline signal. Presence for free.
     */
    @GetMapping("/drivers/{driverId}/location")
    public ResponseEntity<Map<String, Object>> currentLocation(@PathVariable String driverId) {

        Map<Object, Object> hash ;

        try {
            hash = redis.opsForHash().entries("driver:" + driverId + ":loc");
        } catch (Exception e) {
            // CRITICAL distinction: we failed to look, so we must NOT say 404.
            // A 404 means "this driver is definitely offline" — a matching
            // engine would exclude them. During a Redis outage that's a lie
            // about every driver in the fleet.
            throw new SpatialUnavailableException("location lookup failed", e);
        }

        if (hash.isEmpty()) {
            return ResponseEntity.notFound().build();   // never pinged, or gone dark
        }

        return ResponseEntity.ok(Map.of(
                "driverId",  driverId,
                "lat",       Double.parseDouble((String) hash.get("lat")),
                "lng",       Double.parseDouble((String) hash.get("lng")),
                "timestamp", Long.parseLong((String) hash.get("ts")),
                "h3Cell",    hash.get("h3")
        ));
    }

    /**
     * GET /v1/cells/{h3Cell}/count  — supply density in one hex.
     *
     * O(log n) ZCOUNT on a single key. This is the signal Project 3's surge
     * pricing reads. With a GEO index you'd run a radius search and count the
     * results — far more expensive. The cell is a UNIT OF AGGREGATION, not
     * just a search index.
     */
    @GetMapping("/cells/{h3Cell}/count")
    public Map<String, Object> cellCount(@PathVariable String h3Cell) {
        return Map.of(
                "h3Cell", h3Cell,
                "count",  nearbyDriverService.countInCell(h3Cell));
    }

    /**
     * GET /v1/cells/count?lat=&lng=  — same, but resolves the cell for you.
     * Convenience for callers who have coordinates rather than a cell ID.
     */
    @GetMapping("/cells/count")
    public Map<String, Object> cellCountAt(@RequestParam double lat, @RequestParam double lng) {
        String cell = h3IndexService.toStorageCell(lat, lng);
        return Map.of(
                "h3Cell", cell,
                "count",  nearbyDriverService.countInCell(cell));
    }

    /**
     * GET /v1/drivers/{id}/trajectory?from=&to=&cursor=&limit=&sampleSeconds=
     *
     * Served from CASSANDRA, not Redis — this is the "then" question. Note
     * there is no circuit breaker here: unlike the hot path, this is not
     * latency-critical, and Cassandra's own timeouts already bound it.
     * TODO(Phase 7): revisit if history reads get user-facing SLOs.
     */
    @GetMapping("/drivers/{driverId}/trajectory")
    public TrajectoryResponse trajectory(
            @PathVariable String driverId,

            // ISO-8601 instants, e.g. 2026-09-21T00:00:00Z. Explicit UTC in the
            // wire format, matching TimeBuckets — no ambiguity about whose day.
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,

            @RequestParam(required = false) String cursor,

            // Bounded: one page, not one day. 21,600 points would be ~2MB.
            @RequestParam(defaultValue = "1000") @Positive @Max(5000) int limit,

            // Optional downsampling for map rendering.
            @RequestParam(required = false) @Positive @Max(3600) Integer sampleSeconds) {

        if (from.isAfter(to)) {
            throw new IllegalArgumentException("from must be before to");
        }

        return trajectoryService.trajectory(driverId, from, to, cursor, limit, sampleSeconds);
    }

    /**
     * GET /v1/cells/{h3Cell}/occupancy?from=&to=&cursor=&limit=
     *
     * Contrast with GET /v1/cells/{h3}/count, which answers "how many are here
     * RIGHT NOW" from Redis in O(log n). This is the same question asked of the
     * past, and it costs a partition read per hour in the range. Two stores,
     * two questions, separated by time — exactly the split from Session 0.1.
     */
    @GetMapping("/cells/{h3Cell}/occupancy")
    public OccupancyResponse occupancy(
            @PathVariable String h3Cell,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "1000") @Positive @Max(5000) int limit) {

        if (from.isAfter(to)) {
            throw new IllegalArgumentException("from must be before to");
        }
        return occupancyService.occupancy(h3Cell, from, to, cursor, limit);
    }
}