package com.geopulse.ingestion.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Metrics for the write path.
 *
 * Counters are created ONCE in the constructor, not per call. Meter lookup is
 * a map hit with tag resolution — cheap, but not free at 250k/sec, and doing
 * it per request is a needless allocation on the hot path.
 *
 * NOTE the tags: `reason` and `result` have a handful of values each. There is
 * deliberately NO driverId or h3Cell tag — the natural key of the domain is the
 * worst possible metric label. A million drivers would be a million time series.
 */
@Component
public class IngestionMetrics {

    private final Counter accepted;
    private final Counter droppedLowAccuracy;
    private final Counter rejectedValidation;
    private final Counter publishFailed;
    private final Timer   ingestLatency;

    public IngestionMetrics(MeterRegistry registry) {

        this.accepted = Counter.builder("geopulse.pings.accepted")
                .description("Pings that passed validation and the quality gate")
                .register(registry);

        // Dropped != rejected, and the distinction matters operationally:
        // dropped means WE discarded good-faith data (the quality gate);
        // rejected means the CLIENT sent something invalid. A spike in one is
        // a GPS or coverage problem; a spike in the other is a client bug.
        this.droppedLowAccuracy = Counter.builder("geopulse.pings.dropped")
                .tag("reason", "low_accuracy")
                .description("Well-formed pings discarded as untrustworthy")
                .register(registry);

        this.rejectedValidation = Counter.builder("geopulse.pings.rejected")
                .tag("reason", "validation")
                .description("Pings rejected before processing")
                .register(registry);

        // Replaces the TODO from Session 1.3: if Kafka goes down, EVERY send
        // fails, and per-failure logging would be a self-inflicted DoS.
        // Counting is an in-memory increment scraped periodically.
        this.publishFailed = Counter.builder("geopulse.kafka.publish.failed")
                .description("Async publish failures (after the 202 was sent)")
                .register(registry);

        this.ingestLatency = Timer.builder("geopulse.ingest.latency")
                .description("Request received -> 202 returned")
                // Export HISTOGRAM BUCKETS, not pre-computed percentiles.
                // Percentiles do NOT aggregate — you cannot average the p99s of
                // three instances to get the fleet p99. Buckets are counters,
                // and counters DO compose, so Prometheus can compute a correct
                // fleet-wide quantile with histogram_quantile().
                .publishPercentileHistogram()
                // Bound the bucket range around our 50ms p99 SLO. Without SLOs,
                // the default buckets span microseconds to minutes and waste
                // cardinality on ranges we'll never occupy.
                .serviceLevelObjectives(
                        Duration.ofMillis(5), Duration.ofMillis(10),
                        Duration.ofMillis(25), Duration.ofMillis(50),
                        Duration.ofMillis(100), Duration.ofMillis(250))
                .register(registry);
    }

    public void accepted()            { accepted.increment(); }
    public void droppedLowAccuracy()  { droppedLowAccuracy.increment(); }
    public void rejectedValidation()  { rejectedValidation.increment(); }
    public void publishFailed()       { publishFailed.increment(); }

    public Timer.Sample startTimer(MeterRegistry registry) {
        return Timer.start(registry);
    }

    public void recordLatency(Timer.Sample sample) {
        sample.stop(ingestLatency);
    }
}