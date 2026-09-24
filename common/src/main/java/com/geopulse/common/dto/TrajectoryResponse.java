package com.geopulse.common.dto;

import java.util.List;

/**
 * A page of trajectory, plus what the caller needs to fetch the next one.
 *
 * Same principle as NearbyResponse (D-50): a bare array can't tell "that's
 * everything" from "here's the first slice of something larger."
 */
public record TrajectoryResponse(
        String driverId,
        List<TrajectoryPoint> points,
        int pointCount,

        /**
         * Opaque cursor for the next page, or null when exhausted.
         *
         * Opaque ON PURPOSE: it encodes (timestamp, driver-day position) today,
         * and we may change that. A client that parses it becomes coupled to our
         * paging internals — so we hand back a token, not a timestamp.
         */
        String nextCursor,

        /** Buckets (days) actually read — i.e. how many partitions this cost. */
        int partitionsRead,

        /** Echoed so the caller knows what was sampled, if anything. */
        Integer sampleSeconds
) {}