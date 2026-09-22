package com.geopulse.common.history;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/**
 * Partition-key buckets for the Cassandra history tables.
 *
 * THIS IS A CONTRACT between every writer and every reader: buckets are part of
 * the primary key, so the same event must map to the same bucket on any
 * machine, at any time. Lives in `common` so the writer (processing-service)
 * and the reader (query-service, Session 4.3) cannot drift apart.
 *
 * Two rules, each guarding against a real failure:
 *
 *  1. UTC, EXPLICITLY — never the JVM default zone. This project's JVM runs with
 *     -Duser.timezone=Asia/Kolkata; a default-zone bucket would put a 20:00 UTC
 *     ping in tomorrow's partition here and today's on a UTC server. IST's
 *     half-hour offset misaligns even HOURLY buckets.
 *
 *  2. From the DEVICE timestamp — never arrival time. Bucket by arrival and a
 *     redelivery crossing midnight computes a DIFFERENT partition key, turning
 *     our idempotent upsert into a duplicate row in another partition.
 *
 * Pure functions of the event, nothing else.
 */
public final class TimeBuckets {

    private TimeBuckets() {}

    /** driver_trajectory bucket: the UTC calendar day of the ping. */
    public static LocalDate day(long epochMillis) {
        return LocalDate.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }

    /**
     * cell_occupancy bucket: the start of the ping's UTC hour.
     * Instant.truncatedTo is epoch-based, so it is inherently zone-free.
     */
    public static Instant hour(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).truncatedTo(ChronoUnit.HOURS);
    }
}