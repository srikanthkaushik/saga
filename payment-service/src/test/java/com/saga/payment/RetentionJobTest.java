package com.saga.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.saga.common.idempotency.ProcessedMessageRepository;
import com.saga.common.outbox.OutboxRepository;
import com.saga.common.retention.RetentionJob;
import com.saga.common.retention.RetentionProperties;
import com.saga.common.test.PostgresSliceTest;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * The shared retention job against payment-service's schema (any service would do: the outbox and processed_message
 * tables are identical everywhere). Rows are inserted with explicit timestamps; "now" is fixed.
 */
@PostgresSliceTest
class RetentionJobTest {

    private static final Instant NOW = Instant.parse("2030-06-01T12:00:00Z");
    private static final Duration OUTBOX_RETENTION = Duration.ofDays(7);
    private static final Duration MARKER_RETENTION = Duration.ofDays(30);

    @Autowired
    private OutboxRepository outbox;
    @Autowired
    private ProcessedMessageRepository processed;
    @Autowired
    private PlatformTransactionManager transactionManager;
    @Autowired
    private JdbcTemplate jdbc;

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @Test
    void deletesOldPublishedRows_keepsRecentOnes_andNeverTouchesUnpublished() {
        UUID oldPublished = outboxRow(NOW.minus(30, ChronoUnit.DAYS), NOW.minus(30, ChronoUnit.DAYS));
        UUID justPastRetention = outboxRow(NOW.minus(8, ChronoUnit.DAYS), NOW.minus(OUTBOX_RETENTION).minusSeconds(1));
        UUID recentPublished = outboxRow(NOW.minus(8, ChronoUnit.DAYS), NOW.minus(OUTBOX_RETENTION).plusSeconds(1));
        UUID ancientUnpublished = outboxRow(NOW.minus(365, ChronoUnit.DAYS), null);

        RetentionJob.Result result = job(1000, 20).run(NOW);

        assertThat(result.outboxRows()).isEqualTo(2);
        assertThat(outboxIds()).doesNotContain(oldPublished, justPastRetention)
                .contains(recentPublished, ancientUnpublished);
        assertThat(registry.get("saga.retention.deleted").tag("table", "outbox").counter().count()).isEqualTo(2);
    }

    @Test
    void deletesMarkersPastTheirOwnRetention() {
        UUID old = marker(NOW.minus(MARKER_RETENTION).minusSeconds(1));
        UUID kept = marker(NOW.minus(MARKER_RETENTION).plusSeconds(1));
        UUID olderThanOutboxRetentionOnly = marker(NOW.minus(10, ChronoUnit.DAYS));

        RetentionJob.Result result = job(1000, 20).run(NOW);

        assertThat(result.processedMessages()).isEqualTo(1);
        assertThat(processed.existsById(old)).isFalse();
        assertThat(processed.existsById(kept)).isTrue();
        assertThat(processed.existsById(olderThanOutboxRetentionOnly)).isTrue();
        assertThat(registry.get("saga.retention.deleted").tag("table", "processed_message").counter().count()).isEqualTo(1);
    }

    @Test
    void aRunIsCappedAtMaxBatches_andTheNextRunContinues() {
        Instant old = NOW.minus(60, ChronoUnit.DAYS);
        IntStream.range(0, 7).forEach(i -> outboxRow(old, old));

        RetentionJob job = job(2, 2);

        assertThat(job.run(NOW).outboxRows()).isEqualTo(4);
        assertThat(job.run(NOW).outboxRows()).isEqualTo(3);
        assertThat(job.run(NOW).outboxRows()).isZero();
    }

    /** With the job's own interval and initial delay, the scheduled entry point honours the switch. */
    @Test
    void disabled_scheduledRunDeletesNothing() {
        Instant old = Instant.now().minus(60, ChronoUnit.DAYS);
        UUID row = outboxRow(old, old);

        new RetentionJob(outbox, processed, transactionManager, properties(false, 1000, 20), registry).scheduledRun();

        assertThat(outboxIds()).contains(row);
    }

    private RetentionJob job(int batchSize, int maxBatches) {
        return new RetentionJob(outbox, processed, transactionManager, properties(true, batchSize, maxBatches), registry);
    }

    private static RetentionProperties properties(boolean enabled, int batchSize, int maxBatches) {
        return new RetentionProperties(enabled, Duration.ofHours(1), Duration.ofMinutes(1),
                OUTBOX_RETENTION, MARKER_RETENTION, batchSize, maxBatches);
    }

    private UUID outboxRow(Instant createdAt, Instant publishedAt) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                        insert into outbox (id, topic, message_key, message_type, payload, created_at, published_at)
                        values (?, 'order.saga.replies', ?, 'PaymentProcessed', '{}', ?, ?)
                        """,
                id, UUID.randomUUID().toString(), Timestamp.from(createdAt),
                publishedAt == null ? null : Timestamp.from(publishedAt));
        return id;
    }

    private UUID marker(Instant processedAt) {
        UUID id = UUID.randomUUID();
        jdbc.update("insert into processed_message (message_id, processed_at) values (?, ?)", id, Timestamp.from(processedAt));
        return id;
    }

    private List<UUID> outboxIds() {
        return jdbc.queryForList("select id from outbox", UUID.class);
    }
}
