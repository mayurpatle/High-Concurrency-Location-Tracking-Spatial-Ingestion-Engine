package com.geopulse.ingestion.kafka;

import com.geopulse.common.model.LocationPing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes enriched pings to Kafka.
 *
 * THE key decision lives in one line below: the message key is the H3 res-7
 * partition cell. Kafka routes via hash(key) % partitionCount, so every ping
 * from one neighbourhood lands on the SAME partition — and therefore is
 * processed by exactly ONE consumer. That single-writer-per-region property
 * is what lets us skip distributed locks entirely (Phase 5).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class LocationPingProducer {

    // KafkaTemplate is thread-safe — one shared instance serves every request
    // thread. The underlying producer maintains its own internal buffer and
    // background I/O thread, which is exactly what makes send() non-blocking.
    private final KafkaTemplate<String, LocationPing> kafkaTemplate;

    @Value("${geopulse.kafka.topic}")
    private String topic;

    /**
     * Publish one ping. Returns immediately — this does NOT wait for the broker.
     *
     * send() appends to the producer's in-memory buffer and returns a future.
     * A background I/O thread batches records (linger.ms=10) and ships them.
     * This is what makes our 202 honest AND fast: we've taken responsibility
     * for the ping, but we haven't blocked the request thread on network I/O.
     */
    public void publish(LocationPing ping) {

        // The partition key. Using h3PartitionCell (res 7, ~a neighbourhood)
        // rather than driverId is the whole locality strategy — see Session 1.2.
        String key = ping.h3PartitionCell();

        kafkaTemplate.send(topic, key, ping)
                // whenComplete registers a CALLBACK — it does not block.
                // The callback runs on the producer's I/O thread, so it must
                // stay cheap. Never do real work here.
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        // A send failed even after retries. We log rather than
                        // propagate: the HTTP response has ALREADY been sent
                        // (202), so there's no caller left to inform.
                        //
                        // Acceptable ONLY because location data self-heals —
                        // the next ping arrives in ~4s. In a payments system
                        // this would need an outbox table or a retry queue.
                        //
                        // TODO(Phase 6): replace with a `pings.publish.failed`
                        // counter. Logging per-failure at 250k/s is a DoS on
                        // ourselves if Kafka goes down and EVERY send fails.
                        log.error("Failed to publish ping for driver={}", ping.driverId(), ex);
                    }
                });
    }
}