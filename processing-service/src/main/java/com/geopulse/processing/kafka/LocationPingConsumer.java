package com.geopulse.processing.kafka;

import com.geopulse.common.model.LocationPing;
import com.geopulse.processing.exception.NonRetryableException;
import com.geopulse.processing.service.DeduplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Consumes location pings in BATCHES.
 *
 * Why batch: each downstream round trip costs ~1ms whether it carries 1 record
 * or 500 — you pay for the TRIP, not the payload. Processing 500 records
 * individually means ~1000 round trips (~1s); batching means ~2 (~30ms).
 * Same insight as the producer's linger.ms, opposite end of the pipe.
 *
 * The partition guarantee still holds: each partition goes to exactly one
 * thread, so all records in a batch from a given partition are ours alone.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class LocationPingConsumer {

    private final DeduplicationService deduplicationService;

    @KafkaListener(
            topics = "${geopulse.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(List<ConsumerRecord<String, LocationPing>> records) {

        // Collect the records worth writing, so we can hand the whole set to
        // Redis/Cassandra in ONE call each rather than looping with I/O inside.
        List<LocationPing> toProcess = new ArrayList<>(records.size());

        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, LocationPing> record = records.get(i);
            LocationPing ping = record.value();

            // ---- Per-record validation ----
            // BatchListenerFailedException carries the INDEX of the bad record.
            // Spring then commits everything BEFORE it and sends only THAT
            // record to the DLT — so one poison pill doesn't cost us the other
            // 499. Without this, the whole batch would be redelivered.
            if (ping == null) {
                throw new BatchListenerFailedException(
                        "Undeserializable record at offset " + record.offset(),
                        new NonRetryableException("null payload"), i);
            }
            if (ping.driverId() == null || ping.h3Cell() == null) {
                throw new BatchListenerFailedException(
                        "Missing required field at offset " + record.offset(),
                        new NonRetryableException("missing field"), i);
            }

            // ---- Dedup ----
            // TODO(perf): this is still one Redis round trip PER RECORD — the
            // last un-batched I/O in this method. Redis supports pipelining
            // SET NX, which would collapse 500 calls into 1. Deferred because
            // it needs a Lua script or executePipelined to read back each
            // result. Phase 6 load testing will tell us whether it matters.
            if (!deduplicationService.claim(ping.driverId(), ping.timestamp())) {
                continue;   // already processed — skip, offset still advances
            }

            toProcess.add(ping);
        }

        if (toProcess.isEmpty()) {
            return;
        }

        // TODO(Phase 3): redisWriter.writeAll(toProcess)     — one pipelined call
        // TODO(Phase 4): cassandraWriter.writeAll(toProcess) — one batched call
        //
        // Both take the LIST, not a single ping. That signature is the whole
        // point of this session: the write layer must be batch-shaped, or the
        // round-trip savings never materialize.

        // TEMPORARY: one line per BATCH, not per record. Even so this dies in
        // Phase 3, replaced by a Micrometer counter — logging on the hot path
        // is blocking disk I/O, which is what this architecture exists to avoid.
        // log.info("Processed batch: received={} written={} partition={}",
        //          records.size(), toProcess.size(), records.get(0).partition());
    }
}