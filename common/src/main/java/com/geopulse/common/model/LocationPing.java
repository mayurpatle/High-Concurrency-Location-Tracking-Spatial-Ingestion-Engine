package com.geopulse.common.model;

/**
 * The DOMAIN MODEL: a validated, internal location event.
 *
 * Everything downstream (Kafka -> processing -> Redis/Cassandra) speaks THIS
 * type, never the DTO. By the time an object of this class exists, it is
 * TRUSTED: coordinates are in range, driverId is present, timestamp is sane.
 *
 * It is also our KAFKA MESSAGE FORMAT — a contract with our own consumers.
 * Keeping it separate from the DTO means we can add internal fields (h3Cell,
 * next session) without touching the public API.
 *
 * Immutable by construction (record) — which matters a lot in a concurrent
 * pipeline: an immutable object can be shared across threads with zero locking.
 * No defensive copies, no data races. Immutability is a CONCURRENCY strategy,
 * not just a style preference.
 */
public record LocationPing(
        String driverId,
        double lat,          // primitives are safe HERE: validation already
        double lng,          // guaranteed these are present and in range
        long   timestamp,    // epoch millis, device clock

        Double speed,        // still boxed — these are genuinely OPTIONAL,
        Double heading,      // so null is a meaningful value ("not reported")
        Double accuracy
) {

    /**
     * Factory: the ONE place a DTO becomes a domain object.
     *
     * Note this lives in the domain model, not the controller — so translation
     * logic is testable in isolation and can't be duplicated across endpoints
     * (the batch endpoint will reuse it).
     *
     * NOTE: kept as a plain static factory for now. In the next session, once
     * H3 lands, this is where the coordinates get enriched into a cell ID —
     * "index space at the ingestion edge" happens right here.
     */
    public static LocationPing from(String driverId, double lat, double lng, long timestamp,
                                    Double speed, Double heading, Double accuracy) {
        return new LocationPing(driverId, lat, lng, timestamp, speed, heading, accuracy);
    }
}