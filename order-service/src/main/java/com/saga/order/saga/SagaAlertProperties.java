package com.saga.order.saga;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param stuckAfterResends a compensating saga counts as stuck once its current compensation command has been
 *                          re-sent this many times (with 30s compensation timeouts, 3 = stuck after ~2 minutes)
 * @param refreshInterval   how often the stuck-saga gauges are recomputed from the database
 */
@ConfigurationProperties("saga.alert")
public record SagaAlertProperties(
        @DefaultValue("3") int stuckAfterResends,
        @DefaultValue("15s") Duration refreshInterval) {
}
