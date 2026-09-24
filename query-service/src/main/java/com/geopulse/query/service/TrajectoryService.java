package com.geopulse.query.service;

import com.geopulse.common.dto.TrajectoryPoint;
import com.geopulse.common.dto.TrajectoryResponse;
import com.geopulse.query.repository.TrajectoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

@RequiredArgsConstructor
@Service
public class TrajectoryService {

    private final TrajectoryRepository repository;

    public TrajectoryResponse trajectory(String driverId, Instant from, Instant to,
                                         String cursor, int limit, Integer sampleSeconds) {

        Instant cursorTs = decodeCursor(cursor);

        // Fetch one extra row: if it comes back, there IS a next page. Cheaper
        // and more honest than a COUNT, which would cost a second query.
        List<TrajectoryPoint> raw = repository.read(driverId, from, to, cursorTs, limit + 1);

        boolean hasMore = raw.size() > limit;
        List<TrajectoryPoint> page = hasMore ? raw.subList(0, limit) : raw;

        // The cursor comes from the RAW page, before downsampling — otherwise
        // the next request would skip everything we sampled away.
        String next = hasMore
                ? encodeCursor(page.get(page.size() - 1).timestamp())
                : null;

        List<TrajectoryPoint> result = sampleSeconds == null
                ? page
                : downsample(page, sampleSeconds);

        return new TrajectoryResponse(driverId, result, result.size(), next,
                countDays(from, to), sampleSeconds);
    }

    /**
     * Keep one point per interval. At 4-second pings a day is 21,600 points
     * (~2MB of JSON) — useless for drawing a line you'll view at city zoom.
     * Points arrive ts DESC, so we walk down and keep one per window.
     */
    private List<TrajectoryPoint> downsample(List<TrajectoryPoint> points, int sampleSeconds) {
        List<TrajectoryPoint> out = new ArrayList<>();
        long windowMillis = sampleSeconds * 1000L;
        long lastKept = Long.MAX_VALUE;

        for (TrajectoryPoint p : points) {
            if (lastKept - p.timestamp() >= windowMillis || out.isEmpty()) {
                out.add(p);
                lastKept = p.timestamp();
            }
        }
        return out;
    }

    /**
     * Base64 so the cursor is OPAQUE. It happens to be a timestamp today; a
     * client that parses it becomes coupled to our paging internals, and we
     * can't change the scheme without breaking them.
     */
    private String encodeCursor(long timestamp) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(Long.toString(timestamp).getBytes());
    }

    private Instant decodeCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) return null;
        try {
            return Instant.ofEpochMilli(
                    Long.parseLong(new String(Base64.getUrlDecoder().decode(cursor))));
        } catch (Exception e) {
            // A malformed cursor is a client error, not a server fault.
            throw new IllegalArgumentException("invalid cursor");
        }
    }

    private int countDays(Instant from, Instant to) {
        return (int) (java.time.temporal.ChronoUnit.DAYS.between(
                com.geopulse.common.history.TimeBuckets.day(from.toEpochMilli()),
                com.geopulse.common.history.TimeBuckets.day(to.toEpochMilli())) + 1);
    }
}