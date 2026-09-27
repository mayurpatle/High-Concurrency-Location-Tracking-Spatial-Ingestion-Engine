package com.geopulse.processing.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class ProcessingMetrics {

    private final Counter deduplicated;
    private final DistributionSummary batchSize;
    private final Timer redisWrite;
    private final Timer cassandraWrite;
    private final Timer endToEndFreshness;

    public ProcessingMetrics(MeterRegistry registry) {

        // Resolves the Session 2.2 TODO. A rising dedup rate is a real signal:
        // it usually means rebalances are happening more often than they should.
        this.deduplicated = Counter.builder("geopulse.pings.deduplicated")
                .description("Pings suppressed as already-processed")
                .register(registry);

        // A DistributionSummary, not a counter: we care about the SHAPE.
        // Batches near 1 mean the batching economics never kick in (Session 2.3);
        // batches pinned at max-poll-records mean we're saturated and falling behind.
        this.batchSize = DistributionSummary.builder("geopulse.consumer.batch.size")
                .description("Records per poll")
                .publishPercentileHistogram()
                .register(registry);

        this.redisWrite = Timer.builder("geopulse.write.latency")
                .tag("store", "redis")
                .publishPercentileHistogram()
                .register(registry);

        this.cassandraWrite = Timer.builder("geopulse.write.latency")
                .tag("store", "cassandra")
                .publishPercentileHistogram()
                .register(registry);

        // THE SLO METRIC. Device timestamp -> written to Redis. This is the
        // only direct measurement of "how stale is 'now'" — everything else
        // (ingest latency, lag) is a component of it.
        //
        // CAVEAT, and it's a real one: this depends on the DEVICE clock, so a
        // skewed client produces a nonsense value. Read the MEDIAN, not the max
        // — the max will be dominated by whichever phone has the worst clock.
        this.endToEndFreshness = Timer.builder("geopulse.freshness")
                .description("Device timestamp -> visible in Redis")
                .publishPercentileHistogram()
                .serviceLevelObjectives(
                        Duration.ofMillis(500), Duration.ofSeconds(1),
                        Duration.ofSeconds(2), Duration.ofSeconds(5))
                .register(registry);
    }

    public void deduplicated()              { deduplicated.increment(); }
    public void recordBatchSize(int n)      { batchSize.record(n); }
    public Timer redisWriteTimer()          { return redisWrite; }
    public Timer cassandraWriteTimer()      { return cassandraWrite; }

    /** Call once per batch, not per record — sampling one ping is enough to
     *  characterise freshness, and it avoids 500 timer updates per batch. */
    public void recordFreshness(long deviceTimestampMillis) {
        long age = System.currentTimeMillis() - deviceTimestampMillis;
        if (age >= 0 && age < Duration.ofMinutes(5).toMillis()) {
            endToEndFreshness.record(Duration.ofMillis(age));
        }
        // Negative or absurd ages are clock skew, not freshness. Recording them
        // would corrupt the distribution with data about phones, not about us.
    }
}