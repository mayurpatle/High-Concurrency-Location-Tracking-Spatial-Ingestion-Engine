package com.geopulse.query.controller;

import com.geopulse.common.dto.NearbyDriver;
import com.geopulse.common.spatial.H3IndexService;
import com.geopulse.query.service.NearbyDriverService;
import jakarta.validation.constraints.*;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

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
    private final StringRedisTemplate redis;
    private final H3IndexService h3IndexService;

    /**
     * GET /v1/drivers/nearby?lat=&lng=&radiusMetres=&limit=
     *
     * THE query this whole system exists to answer — and the primitive
     * Project 2's matching engine will consume.
     */
    @GetMapping("/drivers/nearby")
    public List<NearbyDriver> nearby(
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

        Map<Object, Object> hash = redis.opsForHash().entries("driver:" + driverId + ":loc");

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
}