package com.geopulse.common.spatial;

import com.uber.h3core.H3Core;
import org.springframework.stereotype.Component;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Wraps the H3 library. Every lat/lng -> cell conversion in the system goes
 * through here.
 *
 * Why wrap it rather than call H3Core directly?
 *  - H3Core.newInstance() loads a NATIVE library. Doing that per call would be
 *    ruinous at 250k/s; we do it ONCE at startup.
 *  - It keeps H3's API (which changed a lot between v3 and v4) behind our own
 *    stable interface — swapping to S2 later would touch this class only.
 *  - It gives us one place to add metrics/caching later.
 *
 * THREAD SAFETY: H3Core is thread-safe and stateless after construction, so a
 * single shared instance serves all request threads with no locking. That
 * matters — a lock here would be a contention point on the hot path, which is
 * exactly what this architecture exists to avoid.
 */
@Component
public class H3IndexService {

    private final H3Core h3;

    /**
     * Loads the native H3 library ONCE at application startup.
     *
     * We convert the checked IOException into an unchecked one deliberately:
     * if H3 can't load, the app CANNOT do its job, so failing fast at startup
     * is correct. A half-working ingestion service is worse than a dead one.
     */
    public H3IndexService() {
        try {
            this.h3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load the native H3 library", e);
        }
    }

    /**
     * lat/lng -> storage cell (res 9). THE hot-path call — runs once per ping.
     *
     * Returns the cell as a String (H3's canonical 15-char hex form, e.g.
     * "8928308280fffff") rather than the raw long. Why: this value becomes a
     * Redis key, a Kafka message key, and a Cassandra partition key — all
     * string-oriented — and it's far easier to debug in logs and kafka-ui.
     * The cost is a few bytes per message; the readability is worth it.
     * (v4 API: latLngToCellAddress. In v3 this was geoToH3Address.)
     */
    public String toStorageCell(double lat, double lng) {
        return h3.latLngToCellAddress(lat, lng, H3Resolution.STORAGE);
    }

    /**
     * Derive the coarse partition cell (res 7) from a storage cell (res 9).
     *
     * This is H3's hierarchy doing real work: instead of a second lat/lng
     * conversion, we walk UP the tree from the cell we already have.
     * Cheaper, and guaranteed consistent with the storage cell.
     */
    public String toPartitionCell(String storageCell) {
        return h3.cellToParentAddress(storageCell, H3Resolution.PARTITION);
    }

    /**
     * All cells within k rings of the origin, INCLUDING the origin itself.
     * Count = 3k² + 3k + 1  (k=1 -> 7, k=2 -> 19, k=7 -> 169).
     *
     * This is the primitive behind "drivers near me" (Phase 3): convert the
     * rider's position to a cell, expand k rings, fetch those keys from Redis.
     * (v4 API: gridDisk. In v3 this was kRing.)
     */
    public List<String> neighbours(String originCell, int k) {
        return h3.gridDisk(originCell, k);
    }

    /**
     * How many rings to cover a given radius, at the STORAGE resolution.
     *
     * NOTE: this is deliberately an OVER-estimate. H3 cells vary slightly in
     * size across the globe, and a cell's "width" isn't a single number, so we
     * round UP. Returning too FEW rings means silently MISSING nearby drivers —
     * a correctness bug. Returning too many just costs a few extra Redis keys.
     * When a search is approximate, always err toward the recoverable mistake.
     */
    public int ringsForRadius(double radiusMetres) {
        final double approxCellWidthMetres = 300.0;   // res 9 ≈ 300 m across
        return (int) Math.ceil(radiusMetres / approxCellWidthMetres);
    }
}