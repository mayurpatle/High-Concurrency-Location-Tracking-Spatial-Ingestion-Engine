package com.geopulse.query.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.geopulse.common.dto.OccupancyRecord;
import com.geopulse.common.history.TimeBuckets;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Reads cell_occupancy — "who was in this hex, and when?"
 *
 * Same technique as TrajectoryRepository (N parallel single-partition reads),
 * but the buckets are HOURLY, so the ceiling matters more: a week of hourly
 * buckets is 168 concurrent queries. Capped at 24.
 */
@Repository
public class OccupancyRepository {

    /** 24 hourly buckets = one day. Client-controlled work needs a ceiling. */
    private static final int MAX_BUCKETS = 24;

    private final CqlSession session;
    private final PreparedStatement selectBucket;

    public OccupancyRepository(CqlSession session) {
        this.session = session;

        // THE COMPOSITE CURSOR: (ts, driver_id) < (:cursor_ts, :cursor_driver).
        //
        // A plain `ts < cursor` would be WRONG here. Many drivers share a
        // millisecond inside one cell, so if a page ends mid-tie, the next page
        // would skip every remaining row at that exact timestamp — silent data
        // loss in the API, invisible unless you look for it.
        //
        // Cassandra compares clustering-column tuples natively, and because the
        // rows are stored sorted by (ts DESC, driver_id), this is still a SEEK.
        // The clustering key we chose in D-56 for write-side uniqueness turns
        // out to be exactly what read-side paging needs.
        this.selectBucket = session.prepare("""
                SELECT ts, driver_id, lat, lng
                FROM cell_occupancy
                WHERE h3_cell = :h3_cell
                  AND hour_bucket = :hour_bucket
                  AND (ts, driver_id) < (:cursor_ts, :cursor_driver)
                  AND ts >= :from_ts
                LIMIT :row_limit
                """);
    }

    public List<OccupancyRecord> read(String h3Cell, Instant from, Instant to,
                                      Instant cursorTs, String cursorDriver, int limit) {

        Instant effectiveTo = (cursorTs != null && cursorTs.isBefore(to)) ? cursorTs : to;

        // A null driver in the tuple would exclude everything, so on the first
        // page we use a driver id that sorts after any real one. Cassandra
        // compares text lexicographically; \uffff is above any practical id.
        String effectiveDriver = cursorDriver != null ? cursorDriver : "\uffff";

        List<Instant> buckets = bucketsBetween(from, effectiveTo);

        List<CompletableFuture<AsyncResultSet>> inFlight = new ArrayList<>(buckets.size());
        for (Instant bucket : buckets) {
            inFlight.add(session.executeAsync(
                            selectBucket.boundStatementBuilder()
                                    .setString("h3_cell", h3Cell)
                                    .setInstant("hour_bucket", bucket)
                                    .setInstant("cursor_ts", effectiveTo)
                                    .setString("cursor_driver", effectiveDriver)
                                    .setInstant("from_ts", from)
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

        // Buckets are newest-first, and rows within a bucket arrive in
        // clustering order — so consuming in order yields a globally sorted
        // stream without a merge sort.
        List<OccupancyRecord> records = new ArrayList<>(limit);
        for (int i = 0; i < buckets.size() && records.size() < limit; i++) {
            for (Row row : inFlight.get(i).join().currentPage()) {
                if (records.size() >= limit) break;
                Instant ts = row.getInstant("ts");
                records.add(new OccupancyRecord(
                        ts == null ? 0L : ts.toEpochMilli(),
                        row.getString("driver_id"),
                        row.getDouble("lat"),
                        row.getDouble("lng")));
            }
        }
        return records;
    }

    /** Hour buckets covering the range, NEWEST FIRST, UTC, capped. */
    private List<Instant> bucketsBetween(Instant from, Instant to) {
        Instant first = TimeBuckets.hour(from.toEpochMilli());
        Instant last  = TimeBuckets.hour(to.toEpochMilli());

        List<Instant> buckets = new ArrayList<>();
        for (Instant b = last; !b.isBefore(first) && buckets.size() < MAX_BUCKETS;
             b = b.minus(1, ChronoUnit.HOURS)) {
            buckets.add(b);
        }
        return buckets;
    }
}