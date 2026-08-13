package com.geopulse.processing.kafka;

import com.geopulse.common.model.LocationPing;
import com.geopulse.processing.exception.NonRetryableException;
import com.geopulse.processing.service.DeduplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes location pings.
 *
 * Correctness property this relies on: each partition is assigned to exactly
 * ONE thread, processing records sequentially in offset order. Combined with
 * our res-7 partition key, one thread owns all of a neighbourhood's traffic.
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
    public void consume(ConsumerRecord<String, LocationPing> record) {

        LocationPing ping = record.value();

        // A null value means ErrorHandlingDeserializer caught a malformed
        // message. We now THROW instead of silently returning (as we did in
        // 2.1) — the error handler routes it to the DLT, where it can be
        // inspected. A dropped message is a mystery; a quarantined one is a bug
        // report. NonRetryableException skips retries: bad bytes will never
        // become valid JSON on the 4th attempt.
        if (ping == null) {
            throw new NonRetryableException(
                    "Undeserializable record at partition=" + record.partition()
                            + " offset=" + record.offset());
        }

        // Defensive validation. These fields are non-null by construction in
        // OUR producer — so a violation means either a schema change or a
        // foreign producer on our topic. Either way, permanently broken.
        if (ping.driverId() == null || ping.h3Cell() == null) {
            throw new NonRetryableException(
                    "Missing required field for offset=" + record.offset());
        }

        // ---- DEDUP ----
        // Claim BEFORE writing. The tradeoff (see session doc): a crash between
        // claiming and writing loses this ping, because redelivery is now
        // suppressed. We accept rare loss over rare duplication, because
        // location self-heals in 4s while corrupted history does not.
        if (!deduplicationService.claim(ping.driverId(), ping.timestamp())) {
            // TODO(Phase 6): counter `pings.deduplicated`. Counting > logging.
            return;   // already processed — offset still commits, we move on
        }

        // TODO(Phase 3): write current location to Redis
        // TODO(Phase 4): append to Cassandra history
        //
        // Any exception from those writes propagates. That's INTENTIONAL:
        // a thrown exception blocks the offset commit and triggers redelivery
        // (at-least-once), and transient failures get retried with backoff by
        // DefaultErrorHandler. Catching and swallowing here would silently
        // convert at-least-once into at-most-once.

        // TEMPORARY — dies in Session 2.3.
        log.info("partition={} offset={} driver={} h3={}",
                record.partition(), record.offset(), ping.driverId(), ping.h3Cell());
    }
}