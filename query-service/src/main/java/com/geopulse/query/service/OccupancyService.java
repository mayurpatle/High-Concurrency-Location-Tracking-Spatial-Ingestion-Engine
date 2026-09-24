package com.geopulse.query.service;

import com.geopulse.common.dto.OccupancyRecord;
import com.geopulse.common.dto.OccupancyResponse;
import com.geopulse.query.repository.OccupancyRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class OccupancyService {

    private final OccupancyRepository repository;

    public OccupancyResponse occupancy(String h3Cell, Instant from, Instant to,
                                       String cursor, int limit) {

        String[] decoded = decodeCursor(cursor);
        Instant cursorTs = decoded == null ? null : Instant.ofEpochMilli(Long.parseLong(decoded[0]));
        String cursorDriver = decoded == null ? null : decoded[1];

        // +1 to detect a next page without a second query.
        List<OccupancyRecord> raw = repository.read(h3Cell, from, to, cursorTs, cursorDriver, limit + 1);

        boolean hasMore = raw.size() > limit;
        List<OccupancyRecord> page = hasMore ? raw.subList(0, limit) : raw;

        String next = null;
        if (hasMore && !page.isEmpty()) {
            OccupancyRecord lastRecord = page.get(page.size() - 1);
            // BOTH components — a timestamp alone can't resume mid-tie.
            next = encodeCursor(lastRecord.timestamp(), lastRecord.driverId());
        }

        Set<String> distinct = new HashSet<>();
        page.forEach(r -> distinct.add(r.driverId()));

        return new OccupancyResponse(h3Cell, page, page.size(), distinct.size(),
                next, countBuckets(from, to));
    }

    private String encodeCursor(long timestamp, String driverId) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString((timestamp + "|" + driverId).getBytes());
    }

    private String[] decodeCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) return null;
        try {
            String decoded = new String(Base64.getUrlDecoder().decode(cursor));
            String[] parts = decoded.split("\\|", 2);
            if (parts.length != 2) throw new IllegalArgumentException();
            return parts;
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid cursor");
        }
    }

    private int countBuckets(Instant from, Instant to) {
        return (int) (ChronoUnit.HOURS.between(
                com.geopulse.common.history.TimeBuckets.hour(from.toEpochMilli()),
                com.geopulse.common.history.TimeBuckets.hour(to.toEpochMilli())) + 1);
    }
}