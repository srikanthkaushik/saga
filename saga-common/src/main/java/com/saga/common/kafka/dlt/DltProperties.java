package com.saga.common.kafka.dlt;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param metricsRefreshInterval how often {@code saga.dlt.pending} is recomputed from Kafka. Each refresh opens a
 *                               short-lived consumer per owned DLT, so keep it well above a few seconds in production
 */
@ConfigurationProperties("saga.dlt")
public record DltProperties(
        @DefaultValue("30s") Duration metricsRefreshInterval) {
}
