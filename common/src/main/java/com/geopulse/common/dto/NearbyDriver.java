package com.geopulse.common.dto;

/**
 * One driver in a proximity result.
 *
 * Carries the true computed distance, not just the position — the caller
 * (matching, in Project 2) needs it for ranking and shouldn't recompute it.
 */
public record NearbyDriver(
        String driverId,
        double lat,
        double lng,
        double distanceMetres,   // TRUE great-circle distance, not cell-based
        long   lastSeenMillis,   // device timestamp — lets the caller judge staleness
        Double heading           // nullable; Project 2 uses it for direction-aware matching
) {}