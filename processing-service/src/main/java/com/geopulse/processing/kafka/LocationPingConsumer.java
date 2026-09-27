package com.geopulse.processing.kafka;

import com.geopulse.common.model.LocationPing;
import com.geopulse.processing.exception.NonRetryableException;
import com.geopulse.processing.metrics.ProcessingMetrics;
import com.geopulse.processing.service.CellActivityTracker;
import com.geopulse.processing.service.DeduplicationService;
import com.geopulse.processing.service.RedisLocationWriter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Consumes location pings in BATCHES — the hot path.
 *
 * Why batch: each downstream round trip costs ~1ms whether it carries 1 record
 * or 500 — you pay for the TRIP, not the payload. Processing 500 records
 * individually means ~1000 round trips (~1s); batching means ~2 (~30ms).
 * Same insight as the producer's linger.ms, opposite end of the pipe.
 *
 * The partition guarantee holds: each partition goes to exactly one thread, so
 * all records in a batch from a given partition are ours alone — which is what
 * lets the writer do an unguarded read-modify-write and the activity tracker
 * use a plain HashMap.
 */
@Component
@RequiredArgsConstructor
public class LocationPingConsumer {

    private final DeduplicationService deduplicationService;
    private final RedisLocationWriter redisLocationWriter;
    private final CellActivityTracker activityTracker;
    private final ProcessingMetrics metrics;

    @KafkaListener(
            topics = "${geopulse.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(List<ConsumerRecord<String, LocationPing>> records) {

        metrics.recordBatchSize(records.size());

        // ---- PASS 1: validate and collect ----
        List<LocationPing> candidates = new ArrayList<>(records.size());

        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, LocationPing> record = records.get(i);
            LocationPing ping = record.value();

            if (ping == null) {
                flush(candidates);   // honour the BatchListenerFailedException promise
                throw new BatchListenerFailedException(
                        "Undeserializable record at offset " + record.offset(),
                        new NonRetryableException("null payload"), i);
            }
            if (ping.driverId() == null || ping.h3Cell() == null) {
                flush(candidates);
                throw new BatchListenerFailedException(
                        "Missing required field at offset " + record.offset(),
                        new NonRetryableException("missing field"), i);
            }

            candidates.add(ping);
        }

        if (candidates.isEmpty()) {
            return;
        }

        // ---- PASS 2: dedup the whole batch in ONE round trip ----
        // Was 500 sequential round trips (~150ms). Now one.
        List<Boolean> claimed = deduplicationService.claimAll(candidates);

        List<LocationPing> toProcess = new ArrayList<>(candidates.size());
        for (int i = 0; i < candidates.size(); i++) {
            if (claimed.get(i)) {
                toProcess.add(candidates.get(i));
            } else {
                metrics.deduplicated();
            }
        }

        if (toProcess.isEmpty()) {
            return;
        }

        metrics.redisWriteTimer().record(() -> redisLocationWriter.writeAll(toProcess));
        metrics.recordFreshness(toProcess.get(0).timestamp());

        int partition = records.get(0).partition();
        for (LocationPing ping : toProcess) {
            activityTracker.record(partition, ping);
        }
        activityTracker.flushPartition(partition);
    }


    /**
     * Flush before throwing a BatchListenerFailedException.
     *
     * Its index is a PROMISE that records 0..i-1 were processed, and Spring
     * commits their offsets on that basis. But note what changed: these
     * candidates have NOT been deduped yet, so flushing them here may write a
     * duplicate. That's acceptable — the Redis write is idempotent (ZADD +
     * stale-write guard), and losing the records entirely would not be.
     *
     * A subtle consequence of batching dedup: the error path now trades a
     * possible duplicate for guaranteed delivery. The old per-record version
     * had already claimed them.
     */
    private void flush(List<LocationPing> pings) {
        if (!pings.isEmpty()) {
            metrics.redisWriteTimer().record(() -> redisLocationWriter.writeAll(pings));
        }
    }
}