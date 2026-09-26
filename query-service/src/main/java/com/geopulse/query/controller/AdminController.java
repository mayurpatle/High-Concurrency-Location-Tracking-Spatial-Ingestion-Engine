package com.geopulse.query.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

/**
 * Operational visibility into hot-cell detection.
 *
 * Not a product API — this is for operators and tests. In production it would
 * live behind an actuator endpoint or an internal-only route, not on the public
 * query surface. TODO(Phase 7): move to /actuator and restrict.
 */
@RestController
@RequestMapping("/v1/admin")
@RequiredArgsConstructor
public class AdminController {

    private static final String ACTIVITY_PREFIX = "cell:activity:";
    private static final String HOT_CELLS_KEY   = "hotcells:current";

    private final StringRedisTemplate redis;

    /**
     * Returns the detector's verdict AND the raw counts it was computed from,
     * so a caller can verify the decision rather than just trusting it.
     */
    @GetMapping("/hot-cells")
    public Map<String, Object> hotCells() {
        Set<String> hot = redis.opsForSet().members(HOT_CELLS_KEY);

        Map<String, Long> activity = new HashMap<>();
        Set<String> keys = redis.keys(ACTIVITY_PREFIX + "*");
        if (keys != null && !keys.isEmpty()) {
            List<String> keyList = new ArrayList<>(keys);
            List<String> values = redis.opsForValue().multiGet(keyList);
            for (int i = 0; values != null && i < keyList.size(); i++) {
                if (values.get(i) != null) {
                    activity.put(keyList.get(i).substring(ACTIVITY_PREFIX.length()),
                            Long.parseLong(values.get(i)));
                }
            }
        }

        return Map.of(
                "hotCells", hot == null ? Set.of() : hot,
                "activity", activity,
                "activeCells", activity.size());
    }
}