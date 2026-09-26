package com.geopulse.processing.config;

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

/**
 * Redefines the DEFAULT listener factory so the hot path can attach a rebalance
 * listener.
 *
 * Spring Boot's auto-configured factory is conditional on a bean NAMED
 * "kafkaListenerContainerFactory" being absent — so declaring one with that
 * exact name replaces it. The configurer then applies everything under
 * spring.kafka.listener (batch mode, ack-mode, concurrency) plus the
 * application's CommonErrorHandler, so we inherit all of it and add only the
 * rebalance listener.
 *
 * The NAME is load-bearing: rename this method and Boot quietly creates its own
 * factory alongside ours, and @KafkaListener picks Boot's — silently dropping
 * the rebalance listener with no error.
 */
@Configuration
public class ActivityTrackingConfig {

    @Bean(name = "kafkaListenerContainerFactory")
    @Primary
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            ConsumerAwareRebalanceListener rebalanceListener) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);

        // Flush in-memory per-cell counters before a partition changes hands.
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);

        return factory;
    }
}