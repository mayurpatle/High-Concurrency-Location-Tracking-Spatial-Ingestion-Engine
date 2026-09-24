package com.geopulse.common.dto;

/**
 * One driver's presence in a cell at a moment in time.
 *
 * Note (timestamp, driverId) together identify this row — a single timestamp
 * does NOT, because many drivers ping in the same millisecond inside one cell.
 * That's the D-56 uniqueness requirement surfacing in the API contract.
 */
public record OccupancyRecord(
        long   timestamp,
        String driverId,
        double lat,
        double lng
) {}