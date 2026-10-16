package com.saga.common.kafka.dlt;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * {@code saga.dlt.pending{topic, dlt}}: dead-lettered records not yet replayed, one gauge per DLT this service owns.
 * The value is the same "pending" that {@code GET /actuator/dlt} reports, i.e. it drops to 0 after a replay even if
 * a replayed record fails again (it then comes back as a new pending record).
 * <p>
 * Refreshed on its own thread, not per scrape and not on the shared {@code @Scheduled} thread: while Kafka is
 * unreachable a refresh blocks for the consumer's API timeout, and that must never delay the outbox relay or the
 * saga timeout scanner. A failed refresh keeps the last value. Gauges are registered on the first successful
 * refresh, so a service that has never reached Kafka has no {@code saga_dlt_pending} series (alerted on as absent).
 * <p>
 * Every instance of a service reports the same Kafka-wide value: aggregate with {@code max by (topic)}, not
 * {@code sum}.
 */
@Component
public class DltMetrics implements DisposableBean {

    public static final String METRIC = "saga.dlt.pending";

    private static final Logger log = LoggerFactory.getLogger(DltMetrics.class);

    private final DltReplayer replayer;
    private final OwnedTopics ownedTopics;
    private final MeterRegistry registry;
    private final DltProperties properties;
    private final Map<String, AtomicLong> pending = new ConcurrentHashMap<>();
    private volatile ScheduledExecutorService executor;

    public DltMetrics(DltReplayer replayer, OwnedTopics ownedTopics, MeterRegistry registry, DltProperties properties) {
        this.replayer = replayer;
        this.ownedTopics = ownedTopics;
        this.registry = registry;
        this.properties = properties;
    }

    /** Listener containers (and therefore the owned topics) are only known once the context is up. */
    @EventListener(ApplicationReadyEvent.class)
    public void startRefreshing() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "dlt-metrics");
            thread.setDaemon(true);
            return thread;
        });
        long intervalMillis = properties.metricsRefreshInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::refresh, 0, intervalMillis, TimeUnit.MILLISECONDS);
        executor = scheduler;
    }

    public void refresh() {
        for (String topic : ownedTopics.get()) {
            try {
                record(replayer.status(topic));
            } catch (RuntimeException e) {
                log.warn("Refreshing {} for {} failed: {}", METRIC, topic, e.getMessage());
            }
        }
    }

    /** Also called with the result of a status read or replay, so the gauge follows operator actions immediately. */
    public void record(DltStatus status) {
        pending.computeIfAbsent(status.topic(), topic -> register(topic, status.dltTopic())).set(status.pending());
    }

    private AtomicLong register(String topic, String dltTopic) {
        AtomicLong value = new AtomicLong();
        Gauge.builder(METRIC, value, AtomicLong::get)
                .description("Dead-lettered records not yet replayed")
                .tag("topic", topic)
                .tag("dlt", dltTopic)
                .register(registry);
        return value;
    }

    @Override
    public void destroy() {
        ScheduledExecutorService scheduler = executor;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
