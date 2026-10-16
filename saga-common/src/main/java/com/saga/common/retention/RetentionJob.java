package com.saga.common.retention;

import java.time.Instant;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.saga.common.SagaCommonConfig;
import com.saga.common.idempotency.ProcessedMessageRepository;
import com.saga.common.outbox.OutboxRepository;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Deletes published outbox rows and idempotency markers past their retention ({@link RetentionProperties}).
 * <p>
 * Deletes run in batches, one transaction each, so locks stay short and a failure loses at most one batch. Rows are
 * picked with {@code FOR UPDATE SKIP LOCKED}, so several instances of a service can run the job at the same time
 * without blocking each other or the outbox relay. A run stops after {@code max-batches-per-run}; the rest waits
 * for the next run.
 * <p>
 * {@code saga.retention.deleted{table}} counts deleted rows.
 */
@Component
public class RetentionJob {

    private static final Logger log = LoggerFactory.getLogger(RetentionJob.class);

    private final OutboxRepository outboxRepository;
    private final ProcessedMessageRepository processedMessageRepository;
    private final TransactionTemplate transactions;
    private final RetentionProperties properties;
    private final Counter outboxDeleted;
    private final Counter processedDeleted;

    public RetentionJob(OutboxRepository outboxRepository,
                        ProcessedMessageRepository processedMessageRepository,
                        PlatformTransactionManager transactionManager,
                        RetentionProperties properties,
                        MeterRegistry registry) {
        this.outboxRepository = outboxRepository;
        this.processedMessageRepository = processedMessageRepository;
        this.transactions = new TransactionTemplate(transactionManager);
        this.properties = properties;
        this.outboxDeleted = deletedCounter(registry, "outbox");
        this.processedDeleted = deletedCounter(registry, "processed_message");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void checkSettings() {
        if (properties.processedMessageRetention().compareTo(SagaCommonConfig.DLT_RETENTION) < 0) {
            log.warn("saga.retention.processed-message-retention ({}) is shorter than the DLT retention ({}): a record "
                            + "replayed from a DLT after its marker is deleted is handled a second time",
                    properties.processedMessageRetention(), SagaCommonConfig.DLT_RETENTION);
        }
    }

    @Scheduled(fixedDelayString = "${saga.retention.interval:1h}", initialDelayString = "${saga.retention.initial-delay:1m}")
    public void scheduledRun() {
        if (!properties.enabled()) {
            return;
        }
        try {
            run(Instant.now());
        } catch (RuntimeException e) {
            // try again next run; nothing is lost by waiting
            log.warn("Retention cleanup failed: {}", e.getMessage());
        }
    }

    public Result run(Instant now) {
        long outbox = purge(now.minus(properties.outboxRetention()), outboxRepository::deletePublishedBefore, outboxDeleted);
        long processed = purge(now.minus(properties.processedMessageRetention()),
                processedMessageRepository::deleteProcessedBefore, processedDeleted);
        if (outbox > 0 || processed > 0) {
            log.info("Retention cleanup deleted {} published outbox row(s) and {} processed-message marker(s)", outbox, processed);
        }
        return new Result(outbox, processed);
    }

    private long purge(Instant cutoff, BiFunction<Instant, Integer, Integer> deleteBatch, Counter counter) {
        long total = 0;
        for (int batch = 0; batch < properties.maxBatchesPerRun(); batch++) {
            Integer deleted = transactions.execute(status -> deleteBatch.apply(cutoff, properties.batchSize()));
            int count = deleted == null ? 0 : deleted;
            total += count;
            counter.increment(count);
            if (count < properties.batchSize()) {
                break;
            }
        }
        return total;
    }

    private static Counter deletedCounter(MeterRegistry registry, String table) {
        return Counter.builder("saga.retention.deleted")
                .description("Rows deleted by the retention cleanup")
                .tag("table", table)
                .register(registry);
    }

    /** Rows deleted by one run. */
    public record Result(long outboxRows, long processedMessages) {
    }
}
