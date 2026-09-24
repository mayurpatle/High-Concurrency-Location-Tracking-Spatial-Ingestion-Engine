package com.geopulse.common.dto;

import java.util.List;

public record OccupancyResponse(
        String h3Cell,
        List<OccupancyRecord> records,
        int recordCount,

        /** Distinct drivers seen in this page — usually the real question. */
        int distinctDrivers,

        String nextCursor,

        /** Hour buckets read — the partition cost, made visible. */
        int partitionsRead
) {}