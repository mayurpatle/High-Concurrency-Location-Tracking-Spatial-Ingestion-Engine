package com.geopulse.ingestion.controller;

import com.geopulse.common.spatial.H3IndexService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * TEMPORARY debug endpoint — makes H3 indexing visible.
 *
 * DELETE THIS in Phase 3, when query-service owns spatial reads. It lives in
 * ingestion-service only because that's where H3IndexService is already wired.
 * A debug endpoint on a public ingestion service is a smell; keeping it
 * short-lived and clearly labelled is the compromise.
 */
@RestController
@RequestMapping("/v1/debug/spatial")
@RequiredArgsConstructor
public class SpatialDebugController {

    private final H3IndexService h3IndexService;

    /** GET /v1/debug/spatial/cell?lat=19.0760&lng=72.8776 */
    @GetMapping("/cell")
    public Map<String, Object> cell(@RequestParam double lat, @RequestParam double lng) {
        String storage = h3IndexService.toStorageCell(lat, lng);
        return Map.of(
                "lat", lat,
                "lng", lng,
                "storageCell",   storage,                              // res 9
                "partitionCell", h3IndexService.toPartitionCell(storage) // res 7
        );
    }

    /** GET /v1/debug/spatial/neighbours?lat=..&lng=..&radiusMetres=2000 */
    @GetMapping("/neighbours")
    public Map<String, Object> neighbours(@RequestParam double lat,
                                          @RequestParam double lng,
                                          @RequestParam(defaultValue = "2000") double radiusMetres) {
        String origin = h3IndexService.toStorageCell(lat, lng);
        int k = h3IndexService.ringsForRadius(radiusMetres);
        List<String> cells = h3IndexService.neighbours(origin, k);

        return Map.of(
                "origin",       origin,
                "radiusMetres", radiusMetres,
                "rings",        k,
                "cellCount",    cells.size(),   // should equal 3k² + 3k + 1
                "cells",        cells
        );
    }
}