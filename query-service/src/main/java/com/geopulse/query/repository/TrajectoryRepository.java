package com.geopulse.query.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.geopulse.common.dto.TrajectoryPoint;
import com.geopulse.common.history.TimeBuckets;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Reads driver_trajectory.
 *
 * THE CORE TECHNIQUE: a multi-day query is N PARALLEL SINGLE-PARTITION reads,
 * driven by us — never `day IN (...)`, which routes through one coordinator
 * that fans out, gathers everything in memory, and returns it. That's the BATCH
 * anti-pattern (D-64) on the read side: a middleman who makes all the trips
 * anyway.
 */
@Repository
public class TrajectoryRepository {

    /** Ceiling on client-controlled work. 30 days = 30 concurrent queries. */
    private static final int MAX_DAYS = 30;

    private final CqlSession session;
    private final PreparedStatement selectDay;

    public TrajectoryRepository(CqlSession session) {
        this.session = session;

        // Bounded by the full partition key + a clustering range — a seek and a
        // forward read, never a scan. The LIMIT bounds one partition's return.
        //
        // ts < :cursorTs is the CURSOR. Rows are physically stored ts DESC, so
        // "everything after the last row I showed you" is a SEEK, not a scan —
        // which is why cursor paging costs the same at page 500 as at page 1.
        this.selectDay = session.prepare("""
                SELECT ts, lat, lng, speed, heading
                FROM driver_trajectory
                WHERE driver_id = :driver_id
                  AND day = :day
                  AND ts < :cursor_ts
                  AND ts >= :from_ts
                LIMIT :row_limit
                """);
    }

    /**
     * Read up to `limit` points, newest first, walking backwards through days.
     *
     * Days are read in PARALLEL but consumed in ORDER — we fire everything at
     * once (latency ≈ the slowest single read), then walk newest-day-first so
     * the LIMIT applies to a correctly ordered stream.
     */
    public List<TrajectoryPoint> read(String driverId, Instant from, Instant to,
                                      Instant cursor, int limit) {

        Instant effectiveTo = (cursor != null && cursor.isBefore(to)) ? cursor : to;
        List<LocalDate> days = daysBetween(from, effectiveTo);

        // Fire one query per day, all at once.
        List<CompletableFuture<AsyncResultSet>> inFlight = new ArrayList<>(days.size());
        for (LocalDate day : days) {
            inFlight.add(session.executeAsync(
                            selectDay.boundStatementBuilder()
                                    .setString("driver_id", driverId)
                                    .setLocalDate("day", day)
                                    // Exclusive upper bound: the cursor row was already
                                    // returned in the previous page.
                                    .setInstant("cursor_ts", effectiveTo)
                                    .setInstant("from_ts", from)
                                    // Over-fetch per partition: each day might hold the
                                    // entire page. Trimmed after the merge.
                                    .setInt("row_limit", limit)
                                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                                    .build())
                    .toCompletableFuture());
        }

        try {
            CompletableFuture.allOf(inFlight.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) throw cause;
            throw e;
        }

        // Merge newest-day-first so the global ordering is ts DESC.
        // Within a day the driver already returns rows in clustering order.
        List<TrajectoryPoint> points = new ArrayList<>(limit);
        for (int i = 0; i < days.size() && points.size() < limit; i++) {
            AsyncResultSet rs = inFlight.get(i).join();
            for (Row row : rs.currentPage()) {
                if (points.size() >= limit) break;
                points.add(toPoint(row));
            }
            // NOTE: we read only currentPage(). The driver would page further
            // (5,000 rows at a time) via fetchNextPage(), but our LIMIT is far
            // below that — and driver paging is INTRA-QUERY state that can't
            // safely cross an HTTP request. Our cursor is the domain-level
            // equivalent, built from data the client can hold and hand back.
        }

        // Defensive: the parallel merge assumes newest-first day ordering.
        points.sort(Comparator.comparingLong(TrajectoryPoint::timestamp).reversed());
        return points;
    }

    /** Days covering the range, NEWEST FIRST, in UTC, capped. */
    private List<LocalDate> daysBetween(Instant from, Instant to) {
        LocalDate first = TimeBuckets.day(from.toEpochMilli());
        LocalDate last  = TimeBuckets.day(to.toEpochMilli());

        List<LocalDate> days = new ArrayList<>();
        // Walk backwards: newest day first, so the LIMIT fills from the newest
        // end of the range — matching CLUSTERING ORDER BY (ts DESC).
        for (LocalDate d = last; !d.isBefore(first) && days.size() < MAX_DAYS; d = d.minusDays(1)) {
            days.add(d);
        }
        return days;
    }

    private TrajectoryPoint toPoint(Row row) {
        Instant ts = row.getInstant("ts");
        return new TrajectoryPoint(
                ts == null ? 0L : ts.toEpochMilli(),
                row.getDouble("lat"),
                row.getDouble("lng"),
                // isNull() before get: a primitive getDouble() on an unset
                // column silently returns 0.0 — and 0.0 is a valid speed and a
                // valid heading (due north). Exactly the Double-vs-double trap
                // from Session 1.1, one layer down.
                row.isNull("speed")   ? null : row.getDouble("speed"),
                row.isNull("heading") ? null : row.getDouble("heading"));
    }
}