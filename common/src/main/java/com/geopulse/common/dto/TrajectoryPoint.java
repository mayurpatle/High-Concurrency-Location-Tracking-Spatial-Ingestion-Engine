package com.geopulse.common.dto;

/**
 * One recorded position. Deliberately leaner than LocationPing — h3Cell and
 * accuracy aren't useful for drawing a route, and at 21,600 points/day every
 * omitted field is real bandwidth.
 */
public record TrajectoryPoint(
        long   timestamp,
        double lat,
        double lng,
        Double speed,
        Double heading
) {}
