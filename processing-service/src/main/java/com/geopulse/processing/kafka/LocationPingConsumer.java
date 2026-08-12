package com.geopulse.processing.kafka;

import com.geopulse.common.model.LocationPing;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes location pings from Kafka.
 *
 * THE correctness property this class relies on:
 * each partition is assigned to exactly ONE thread in the group, and that
 * thread processes its records sequentially in offset order. So this method is
 * EFFECTIVELY SINGLE-THREADED PER PARTITION — and since we key by H3 res-7
 * cell, that means one thread owns all of a neighbourhood's traffic.
 *
 * That's the "partition is your lock" guarantee, and it comes from Kafka's
 * assignment model rather than anything we implement. It's why the regional
 * state we add later needs no synchronization.
 */
@Component
@Slf4j
public class LocationPingConsumer {

    /**
     * One record at a time (batch consumption comes in Session 2.3).
     *
     * We take the whole ConsumerRecord rather than just the value so we can see
     * partition and offset — invaluable while learning, and the basis of the
     * metrics we add in Phase 6.
     *
     * IMPORTANT: this method returning normally is what allows the offset to be
     * committed. Throwing prevents the commit, so the record is redelivered —
     * that IS our at-least-once mechanism. Never swallow an exception here
     * unless you genuinely mean "this record is unprocessable, move on."
     */
    @KafkaListener(
            topics = "${geopulse.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, LocationPing> record) {

        LocationPing ping = record.value();

        // The ErrorHandlingDeserializer hands us a null value when a message
        // couldn't be deserialized (the poison-pill guard). Skip it so the
        // offset advances rather than stalling the partition forever.
        // Session 2.2 routes these to a dead-letter topic instead of dropping.
        if (ping == null) {
            log.warn("Skipping undeserializable record at partition={} offset={}",
                    record.partition(), record.offset());
            return;
        }

        // TODO(Session 2.2): dedupe on driverId + timestamp
        // TODO(Phase 3):     write current location to Redis
        // TODO(Phase 4):     append to Cassandra history

        // TEMPORARY — this proves consumption works and makes partition
        // assignment visible. It dies in Session 2.3, for the same reason the
        // producer's log did: disk I/O per record on the hot path is exactly
        // the blocking work this architecture exists to avoid.
        log.info("partition={} offset={} driver={} h3={} partitionCell={}",
                record.partition(), record.offset(),
                ping.driverId(), ping.h3Cell(), ping.h3PartitionCell());
    }
}