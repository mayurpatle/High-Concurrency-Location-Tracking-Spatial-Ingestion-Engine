package com.geopulse.common.dto;

import java.util.List;

/**
 * Nearby results plus the metadata a caller needs to decide what to do next.
 *
 * Why not just return List<NearbyDriver>: a bare array can't distinguish
 * "searched and found nothing" from "found 400 and gave you 50", and a caller
 * that can't tell the difference makes bad decisions.
 *
 * NOTE what's deliberately absent: offset/page. Offset pagination assumes a
 * STABLE ORDERING between requests — but our drivers move every 4 seconds, so
 * page 2 would duplicate some drivers and silently skip others. You cannot
 * paginate a result set that reorders itself. The correct way to get more
 * candidates is to RE-QUERY WITH A LARGER RADIUS, which is a different
 * question with a superset answer.
 */
public record NearbyResponse(
        List<NearbyDriver> drivers,

        /** How many matched within the radius, BEFORE the limit was applied. */
        int totalFound,

        /** True if totalFound > limit — the caller is seeing a subset. */
        boolean truncated,

        /** Echoed back so the caller knows what was actually searched. */
        double radiusMetres,

        /** Cells scanned — useful for debugging and capacity work. */
        int cellsScanned,

        /**
         * The origin cell. Lets a caller correlate this result with
         * /cells/{h3}/count, and gives Project 2 a stable regional handle.
         */
        String originCell
) {}