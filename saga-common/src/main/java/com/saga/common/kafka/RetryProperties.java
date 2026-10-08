package com.saga.common.kafka;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Blocking retry policy for Kafka listeners before a record is dead-lettered. With the defaults a
 * failing record is attempted 5 times over roughly 7.5s (0.5s, 1s, 2s, 4s).
 */
@ConfigurationProperties("saga.kafka.retry")
public record RetryProperties(
        @DefaultValue("4") int maxRetries,
        @DefaultValue("500ms") Duration initialInterval,
        @DefaultValue("2.0") double multiplier,
        @DefaultValue("10s") Duration maxInterval) {
}
