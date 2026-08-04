package com.geopulse.common.spatial;

/**
 * The two H3 resolutions this system uses, and WHY each was chosen.
 *
 * Constants (not config) on purpose: changing a resolution invalidates every
 * stored cell ID in Redis and Cassandra. It is a DATA MIGRATION, not a tunable.
 * Making it look tunable would be a trap.
 */
public final class H3Resolution {

    /**
     * STORAGE / INDEX resolution — ~0.10 km² per cell (~174 m edge).
     *
     * Used as: the Redis spatial index key, and the Cassandra partition key.
     *
     * Why 9: at ~2,000 drivers/km² (dense city) a cell holds ~200 drivers —
     * a useful unit, small enough to fetch whole. A 2 km radius search needs
     * k≈7 rings = 3(7²)+3(7)+1 = 169 cells: one pipelined Redis round trip.
     * Res 10 would need ~1,200 cells; res 8 would fetch ~1,400 drivers per cell
     * and discard most. Res 9 minimizes (cells touched × drivers per cell).
     */
    public static final int STORAGE = 9;

    /**
     * PARTITIONING resolution — ~5.2 km² per cell (~1.2 km edge), a neighbourhood.
     *
     * Used as: the Kafka partition key (Session 1.3).
     *
     * Why coarser than STORAGE: partitioning wants LOCALITY. Too many distinct
     * keys and adjacent areas scatter across partitions, destroying the
     * single-writer-per-region property we're building toward. Too few and we
     * get hot partitions. A neighbourhood is the natural "route this region's
     * traffic to one consumer" unit.
     *
     * Bonus: res 7 is res 9's GRANDPARENT, so we derive it from the storage
     * cell with one cellToParent() call — compute once, get both.
     */
    public static final int PARTITION = 7;

    private H3Resolution() {}   // static-only holder
}