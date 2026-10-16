package com.saga.common.retention;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param enabled                     switch the cleanup job off entirely
 * @param interval                    pause between runs
 * @param initialDelay                first run after startup
 * @param outboxRetention             published outbox rows older than this are deleted. Unpublished rows are never
 *                                    deleted. The console's message timeline for an order disappears with them
 * @param processedMessageRetention   idempotency markers older than this are deleted. A record delivered again after
 *                                    its marker is gone is handled again, so keep this longer than any redelivery:
 *                                    DLT replay (DLT retention 14 days) and consumer-offset resets (source topic
 *                                    retention). The handlers are idempotent on business keys as well, so the
 *                                    marker is the first line of defence, not the only one
 * @param batchSize                   rows deleted per transaction
 * @param maxBatchesPerRun            caps one run so it never holds the shared scheduler thread for long; a backlog
 *                                    is worked off over several runs
 */
@ConfigurationProperties("saga.retention")
public record RetentionProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("1h") Duration interval,
        @DefaultValue("1m") Duration initialDelay,
        @DefaultValue("7d") Duration outboxRetention,
        @DefaultValue("30d") Duration processedMessageRetention,
        @DefaultValue("1000") int batchSize,
        @DefaultValue("20") int maxBatchesPerRun) {
}
