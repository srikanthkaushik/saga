package com.saga.order.saga;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * How long each saga step may wait for its reply. Keep each comfortably above the Kafka retry budget
 * ({@code saga.kafka.retry.*}, ~7.5s by default): a participant that is merely retrying should not time out.
 */
@ConfigurationProperties("saga.timeout")
public record SagaTimeoutProperties(
        @DefaultValue("30s") Duration payment,
        @DefaultValue("30s") Duration inventory,
        @DefaultValue("30s") Duration compensation,
        @DefaultValue("5s") Duration scanInterval,
        @DefaultValue("100") int batchSize) {
}
