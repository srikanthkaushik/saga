package com.saga.order.saga;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Metrics for compensations that are not completing. Gauges are refreshed from the database on a schedule
 * (not per scrape) and reflect the whole database, so with several order-service instances alert on
 * {@code max()}, not {@code sum()}.
 * <ul>
 *   <li>{@code saga.compensation.stuck} - sagas whose compensation has been re-sent >= threshold times</li>
 *   <li>{@code saga.compensation.in.progress} - sagas currently compensating</li>
 *   <li>{@code saga.compensation.oldest.age} (seconds) - how long the oldest compensating saga has been at it</li>
 *   <li>{@code saga.compensation.resends} - counter of compensation re-sends by this instance</li>
 * </ul>
 */
@Component
public class CompensationMonitor {

    private static final Logger log = LoggerFactory.getLogger(CompensationMonitor.class);

    private final OrderSagaRepository sagaRepository;
    private final SagaAlertProperties alerts;
    private final AtomicLong stuck = new AtomicLong();
    private final AtomicLong inProgress = new AtomicLong();
    private final AtomicLong oldestAgeSeconds = new AtomicLong();
    private final Counter resends;

    public CompensationMonitor(OrderSagaRepository sagaRepository, SagaAlertProperties alerts, MeterRegistry registry) {
        this.sagaRepository = sagaRepository;
        this.alerts = alerts;
        Gauge.builder("saga.compensation.stuck", stuck, AtomicLong::get)
                .description("Sagas whose compensation has been re-sent at least saga.alert.stuck-after-resends times")
                .register(registry);
        Gauge.builder("saga.compensation.in.progress", inProgress, AtomicLong::get)
                .description("Sagas currently compensating")
                .register(registry);
        Gauge.builder("saga.compensation.oldest.age", oldestAgeSeconds, AtomicLong::get)
                .description("Age of the oldest in-progress compensation")
                .baseUnit("seconds")
                .register(registry);
        this.resends = Counter.builder("saga.compensation.resends")
                .description("Compensation commands re-sent after a compensation timeout")
                .register(registry);
    }

    /** Called by the orchestrator each time a compensation command is re-sent. */
    void compensationResent(OrderSaga saga, int resendCount) {
        resends.increment();
        if (resendCount == alerts.stuckAfterResends()) {
            log.error("STUCK COMPENSATION: saga {} for order {} in {} has re-sent its compensation {} times "
                            + "(compensating since {}, reason: {}). See GET /actuator/stucksagas",
                    saga.getId(), saga.getOrderId(), saga.getState(), resendCount,
                    saga.getCompensatingSince(), saga.getFailureReason());
        } else if (resendCount > alerts.stuckAfterResends()) {
            log.error("Saga {} for order {} still stuck in {} after {} compensation re-sends",
                    saga.getId(), saga.getOrderId(), saga.getState(), resendCount);
        }
    }

    @Scheduled(fixedDelayString = "${saga.alert.refresh-interval:15s}")
    public void refresh() {
        try {
            OrderSagaRepository.CompensationStats stats = sagaRepository.compensationStats(alerts.stuckAfterResends());
            stuck.set(stats.getStuck());
            inProgress.set(stats.getCompensating());
            oldestAgeSeconds.set((long) stats.getOldestAgeSeconds());
        } catch (RuntimeException e) {
            // keep the last values; a failing DB is alerted on elsewhere
            log.warn("Refreshing compensation metrics failed: {}", e.getMessage());
        }
    }
}
