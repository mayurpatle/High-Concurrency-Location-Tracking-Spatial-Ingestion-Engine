package com.geopulse.processing.config;

import com.geopulse.common.model.LocationPing;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

/**
 * Central error policy for the consumer.
 *
 * The whole point: distinguish TRANSIENT failures (retry — the world was
 * briefly unavailable) from PERMANENT ones (don't retry — the message is
 * broken and never will succeed). Conflating them gives you either infinite
 * loops or silent data loss.
 */
@Configuration
@Slf4j
public class KafkaErrorHandlingConfig {

    /**
     * A DEDICATED KafkaTemplate for dead-letter publishing.
     *
     * Why not the default one: DLT records arrive in two different shapes.
     *   - Deserialization failures -> value is the raw byte[] that failed to parse
     *   - Listener failures        -> value is a fully-deserialized LocationPing
     *
     * A single serializer can't handle both — and Spring Boot's default
     * (StringSerializer for key AND value) handles NEITHER. That failure is
     * vicious: the publish throws -> the recoverer fails -> DefaultErrorHandler
     * puts the record BACK into retry -> forever. Your safety net becomes
     * another infinite loop, and the logs say "not yet recovered" rather than
     * anything about serialization.
     *
     * DelegatingByTypeSerializer picks the right serializer per payload type.
     */
    @Bean
    public KafkaTemplate<Object, Object> dltKafkaTemplate(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        DefaultKafkaProducerFactory<Object, Object> factory = new DefaultKafkaProducerFactory<>(config);
        factory.setValueSerializer(new DelegatingByTypeSerializer(Map.of(
                byte[].class,       new ByteArraySerializer(),
                LocationPing.class, new JsonSerializer<>()
        )));

        return new KafkaTemplate<>(factory);
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> dltKafkaTemplate) {

        // ---- Where exhausted / non-retryable records go ----
        // Republishes the failed record to "<original-topic>.DLT", preserving
        // key, value and headers, and ADDING headers that describe the failure
        // (exception class, message, stack trace, original partition/offset).
        // That metadata is what makes a DLT a bug report rather than a
        // graveyard of mystery bytes.
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(dltKafkaTemplate);

        // ---- Backoff: give a struggling dependency room to recover ----
        // 1s -> 2s -> 4s, then give up. Immediate retries would ADD load to an
        // already-overloaded dependency — the classic retry storm that turns a
        // degradation into an outage.
        //
        // The ceiling exists because retries burn our max.poll.interval.ms
        // budget: 7s total against a 300s deadline. Retry too long and Kafka
        // evicts us for not polling — a transient blip becomes a rebalance storm.
        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxElapsedTime(7000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // ---- Classification: DENY BY DEFAULT ----
        // We list what IS retryable; the trailing `false` makes everything else
        // non-retryable, going straight to the DLT.
        //
        // Why this way round: an exception type we didn't anticipate is far more
        // likely a code defect than a passing network blip. Retrying a defect
        // wastes 7s per record and can stall a partition; quarantining it
        // surfaces the bug immediately. Enumerating what's PERMANENT is
        // unbounded (you can't predict every bug); enumerating what's TRANSIENT
        // is a short, knowable list.
        handler.setClassifications(Map.of(
                org.springframework.dao.QueryTimeoutException.class,               true,
                org.springframework.dao.TransientDataAccessException.class,        true,
                org.springframework.data.redis.RedisConnectionFailureException.class, true,
                SocketTimeoutException.class,                                      true
        ), false);

        // NOTE: failedDelivery fires on attempt 1 even for records headed
        // STRAIGHT to the DLT — so a single line here does NOT prove retries
        // are happening. Watch for repeated attempts (2, 3) to confirm that.
        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("Delivery attempt {} failed for partition={} offset={}: {}",
                        attempt, record.partition(), record.offset(), ex.getMessage()));

        return handler;
    }
}