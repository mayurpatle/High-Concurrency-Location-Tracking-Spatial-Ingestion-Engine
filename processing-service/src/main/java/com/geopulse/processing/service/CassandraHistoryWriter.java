package com.geopulse.processing.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.geopulse.common.history.TimeBuckets;
import com.geopulse.common.model.LocationPing;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Writes pings to the Cassandra history tables — the "then" tier.
 *
 * NOT a Cassandra BATCH. A batch routes everything through one coordinator
 * (plus batchlog writes for a logged batch) and our pings span hundreds of
 * partitions — a middleman that makes every trip anyway, and Cassandra rejects
 * batches over 50 KB by default. Batching only amortizes a fixed cost paid to
 * ONE destination; here there are many. The Cassandra equivalent of pipelining
 * is CONCURRENCY: every write fired async, token-aware, straight to its owner.
 *
 * No dedup needed: both primary keys identify the logical event, so a
 * redelivered ping is an upsert of identical data. At-least-once + idempotent
 * write = exactly-once EFFECT (Session 4.2, Part 1).
 */
@Service
public class CassandraHistoryWriter {

    private final CqlSession session;
    private final PreparedStatement trajectoryInsert;
    private final PreparedStatement occupancyInsert;

    /**
     * Statements are prepared ONCE, here, at startup. Preparing per write adds a
     * round trip and triggers driver warnings.
     *
     * Preparing also enables token-aware routing: the prepared metadata tells
     * the driver which variables form the partition key, so it can compute the
     * token and send each write directly to a replica that owns it.
     *
     * CAVEAT: this (and the driver session itself) needs Cassandra reachable at
     * startup — so while history shares a JVM with the Redis path, Cassandra
     * being down at BOOT also blocks live tracking from starting.
     * TODO(Phase 7): separate deployable removes that startup coupling.
     */
    public CassandraHistoryWriter(CqlSession session) {
        this.session = session;

        // Named bind markers (:driver_id) rather than positional (?) — the
        // binding code below reads as the schema, and can't silently misalign.
        this.trajectoryInsert = session.prepare("""
                INSERT INTO driver_trajectory
                    (driver_id, day, ts, lat, lng, h3_cell, speed, heading, accuracy)
                VALUES
                    (:driver_id, :day, :ts, :lat, :lng, :h3_cell, :speed, :heading, :accuracy)
                """);

        this.occupancyInsert = session.prepare("""
                INSERT INTO cell_occupancy
                    (h3_cell, hour_bucket, ts, driver_id, lat, lng)
                VALUES
                    (:h3_cell, :hour_bucket, :ts, :driver_id, :lat, :lng)
                """);
    }

    /**
     * Fire every write concurrently, then wait for ALL of them.
     *
     * Total latency ≈ the slowest single write, not the sum of 1,000.
     *
     * allOf() waits for every future to finish before failing, so we never
     * rethrow while writes are still in flight — a redelivery can't overlap a
     * half-finished previous attempt.
     *
     * Bounded concurrency: 500 pings × 2 tables = 1,000 requests per batch,
     * per listener thread — enough to exceed the driver's per-connection limit.
     * Part 3 configures the driver's concurrency-limiting throttler.
     * TODO(Phase 6): tune the throttler limits from load-test measurements.
     */
    public void writeAll(List<LocationPing> pings) {
        if (pings.isEmpty()) {
            return;
        }

        List<CompletableFuture<AsyncResultSet>> inFlight = new ArrayList<>(pings.size() * 2);
        for (LocationPing ping : pings) {
            inFlight.add(session.executeAsync(trajectory(ping)).toCompletableFuture());
            inFlight.add(session.executeAsync(occupancy(ping)).toCompletableFuture());
        }

        try {
            CompletableFuture.allOf(inFlight.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException e) {
            // Unwrap so the Kafka error handler classifies the REAL driver
            // exception (e.g. WriteTimeoutException = transient, retry) rather
            // than an opaque CompletionException wrapper.
            if (e.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw e;
        }
    }

    private BoundStatement trajectory(LocationPing ping) {
        BoundStatementBuilder b = trajectoryInsert.boundStatementBuilder()
                .setString("driver_id", ping.driverId())
                .setLocalDate("day", TimeBuckets.day(ping.timestamp()))
                .setInstant("ts", Instant.ofEpochMilli(ping.timestamp()))
                .setDouble("lat", ping.lat())
                .setDouble("lng", ping.lng())
                .setString("h3_cell", ping.h3Cell());

        // Optional fields: set ONLY when present, otherwise leave UNSET.
        // A null in Cassandra is a TOMBSTONE — reads scan past every one, warn
        // above 1,000 and FAIL above 100,000. A driver that never reports these
        // would plant ~65,000 tombstones/day in their partition. Unset values
        // are simply not written.
        if (ping.speed() != null)    b = b.setDouble("speed", ping.speed());
        if (ping.heading() != null)  b = b.setDouble("heading", ping.heading());
        if (ping.accuracy() != null) b = b.setDouble("accuracy", ping.accuracy());

        return finish(b);
    }

    private BoundStatement occupancy(LocationPing ping) {
        return finish(occupancyInsert.boundStatementBuilder()
                .setString("h3_cell", ping.h3Cell())
                .setInstant("hour_bucket", TimeBuckets.hour(ping.timestamp()))
                .setInstant("ts", Instant.ofEpochMilli(ping.timestamp()))
                .setString("driver_id", ping.driverId())
                .setDouble("lat", ping.lat())
                .setDouble("lng", ping.lng()));
    }

    /**
     * Options shared by every history write.
     *
     * LOCAL_QUORUM: history is the durable record — a lost point is a permanent
     * gap, not something the next ping repairs. Affordable because Part 1 moved
     * history OFF the critical path, so its latency never touches freshness.
     * With RF=3 in production, LOCAL_QUORUM writes + LOCAL_QUORUM reads give
     * R + W = 4 > 3 → read-your-writes. (Dev RF=1: quorum of one = one.)
     *
     * Idempotence = true: we PROVED these writes are safe to repeat. Telling
     * the driver lets it retry on another node when a connection fails
     * mid-request, and enables speculative execution — neither of which it
     * will do for a statement it can't assume is repeatable.
     */
    private BoundStatement finish(BoundStatementBuilder b) {
        return b.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                .setIdempotence(true)
                .build();
    }
}