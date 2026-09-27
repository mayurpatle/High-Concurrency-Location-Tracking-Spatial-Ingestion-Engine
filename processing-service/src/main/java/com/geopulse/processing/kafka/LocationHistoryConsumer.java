package com.geopulse.processing.kafka;

import com.geopulse.common.model.LocationPing;
import com.geopulse.processing.exception.NonRetryableException;
import com.geopulse.processing.metrics.ProcessingMetrics;
import com.geopulse.processing.service.CassandraHistoryWriter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * The cold path: its own consumer group, its own offsets, its own lag.
 * A slow or dead Cassandra grows THIS group's lag and never touches the
 * Redis hot path (Session 4.2, Part 1).
 *
 * No dedup: both history primary keys identify the logical event, so a
 * redelivered ping is an upsert of identical data.
 */
@Component
@RequiredArgsConstructor
public class LocationHistoryConsumer {

    private final CassandraHistoryWriter historyWriter;
    private final ProcessingMetrics metrics;

    @KafkaListener(
            topics = "${geopulse.kafka.topic}",
            groupId = "${geopulse.kafka.history-group-id}",
            containerFactory = "historyListenerFactory",
            clientIdPrefix = "history",   // distinguishes this group's members in kafka-ui and logs

            // EARLIEST, explicitly and even in production. The hot path would
            // arguably use `latest` in prod — a 2-hour-old position is WRONG for
            // "now". History wants the opposite: old data is exactly what it's
            // for. Another setting that differs by path.
            properties = "auto.offset.reset=earliest"
    )
    public void consume(List<ConsumerRecord<String, LocationPing>> records) {

        // Not tagged by group — the metric name and the `store` tag already
        // distinguish this from the hot path's Redis writes.
        metrics.recordBatchSize(records.size());

        List<LocationPing> pings = new ArrayList<>(records.size());

        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, LocationPing> record = records.get(i);
            LocationPing ping = record.value();

            if (ping == null || ping.driverId() == null || ping.h3Cell() == null) {
                // BatchListenerFailedException's index is a PROMISE: "every
                // record before index i was processed." Spring commits their
                // offsets on the strength of it. So those records must actually
                // be WRITTEN before we throw — otherwise their offsets are
                // committed for rows that never reached Cassandra, and they are
                // gone for good.
                //
                // Timed as well: excluding the error path would quietly remove
                // the writes most likely to be SLOW from the distribution.
                metrics.cassandraWriteTimer().record(() -> historyWriter.writeAll(pings));

                throw new BatchListenerFailedException(
                        "Unprocessable record at offset " + record.offset(),
                        new NonRetryableException("null or incomplete payload"), i);
            }
            pings.add(ping);
        }

        // Timer.record(Runnable) starts a clock, runs the lambda, stops it, and
        // records the duration. If writeAll throws, the elapsed time is STILL
        // recorded and the exception propagates normally — which is what we
        // want: a write that failed after 3 seconds is important data, and
        // dropping it would make the distribution look healthier than reality.
        //
        // A driver exception here is NOT a BatchListenerFailedException, so the
        // whole batch is retried with the patient backoff from 4.2 — safely,
        // because every write in it is idempotent.
        metrics.cassandraWriteTimer().record(() -> historyWriter.writeAll(pings));
    }
}