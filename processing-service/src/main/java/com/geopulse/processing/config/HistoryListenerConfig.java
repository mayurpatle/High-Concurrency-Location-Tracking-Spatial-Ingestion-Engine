package com.geopulse.processing.config;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Map;

/**
 * Listener factory for the HISTORY consumer group — the cold path.
 *
 * Same Kafka topic as the hot path, different reflex to failure:
 *   hot path  -> quarantine fast (a stale ping is useless)
 *   cold path -> WAIT OUT transient failures; Kafka holds the data for 6h,
 *                so a Cassandra outage should become lag, never quarantine
 */
@Configuration
public class HistoryListenerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> historyListenerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<Object, Object> dltKafkaTemplate,
            @Value("${geopulse.kafka.history-dlt}") String historyDlt) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        // Inherit everything under spring.kafka.listener — batch mode, ack-mode,
        // concurrency — so the two paths can't drift apart on the basics...
        configurer.configure(factory, consumerFactory);

        // ...then override the one thing that MUST differ: patience.
        // Concurrency is inherited but independently tunable per group — one of
        // the things a separate group buys.
        // TODO(Phase 6): size history concurrency from measured Cassandra throughput.
        factory.setCommonErrorHandler(historyErrorHandler(dltKafkaTemplate, historyDlt));
        return factory;
    }

    /**
     * Deliberately NOT a @Bean.
     *
     * Spring Boot wires the default listener factory with the application's
     * CommonErrorHandler only if exactly ONE exists. Declare a second one as a
     * bean and the lookup becomes ambiguous — the DEFAULT factory silently loses
     * its handler, and the hot path's poison messages stop going to the DLT.
     * No error at startup; you'd only notice when a poison pill vanished.
     */
    private DefaultErrorHandler historyErrorHandler(KafkaTemplate<Object, Object> template,
                                                    String historyDlt) {

        // Pinned, per-group DLT: a message can be poison for this consumer and
        // fine for the hot path, and replays must go to the right group.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(historyDlt, -1));

        // PATIENT IN TOTAL, SHORT PER WAIT.
        // Spring's whole-batch retry pauses the consumer and polls before each
        // sleep, so the poll deadline resets between retries — but one attempt
        // plus one wait must still fit inside max.poll.interval.ms (300s).
        //   each wait:  1s, 2s, 4s ... capped at 60s   (≪ 300s)
        //   attempt:    ≤ ~2s (spring.cassandra.request.timeout)
        //   total:      30 minutes — rides out a node restart or rolling
        //               upgrade, but bounded so a MISCLASSIFIED permanent error
        //               can't freeze a partition until 6h retention expires.
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(60_000L);
        backOff.setMaxElapsedTime(1_800_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // DENY BY DEFAULT (D-27), with the Cassandra driver's transient failures
        // opted in. The flip side of deny-by-default: every new dependency is a
        // list you must maintain. Forget this and every Cassandra timeout is
        // dead-lettered as if it were poison.
        handler.setClassifications(Map.of(
                AllNodesFailedException.class,    true,   // unreachable — also covers NoNodeAvailableException
                DriverTimeoutException.class,     true,   // client-side request timeout
                WriteTimeoutException.class,      true,   // replicas didn't ack in time
                UnavailableException.class,       true,   // too few live replicas for the consistency level
                OverloadedException.class,        true,   // coordinator shedding load
                RequestThrottlingException.class, true    // our own throttler's queue is full
        ), false);

        return handler;
    }
}