package com.geopulse.common.spatial;

/**
 * A cell flagged as carrying disproportionate load.
 *
 * In `common` because BOTH sides need it: the detector (processing-service)
 * publishes these, and the partitioner (ingestion-service) consumes them.
 */
public record HotCell(
        String h3PartitionCell,
        long   pingCount,        // in the last observation window
        double ratioToMedian,    // how far above the typical cell
        int    salt              // how many partitions to spread it across (Part 3)
) {}