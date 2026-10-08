package com.saga.common.outbox;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.saga.common.messaging.Topics;

/**
 * Polls unpublished outbox rows and pushes them to Kafka. Rows are locked with SKIP LOCKED so several
 * instances can run concurrently. If any send in a batch fails the transaction rolls back and the whole
 * batch is retried on the next tick: delivery is at-least-once, consumers must be idempotent.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int batchSize;

    public OutboxRelay(OutboxRepository outboxRepository,
                       KafkaTemplate<String, String> kafkaTemplate,
                       @Value("${saga.outbox.batch-size:100}") int batchSize) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${saga.outbox.poll-interval:500ms}")
    @Transactional
    public void publishPending() {
        List<OutboxMessage> batch = outboxRepository.lockUnpublishedBatch(batchSize);
        if (batch.isEmpty()) {
            return;
        }

        CompletableFuture<?>[] sends = batch.stream()
                .map(message -> kafkaTemplate.send(toRecord(message)))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(sends).join();

        batch.forEach(OutboxMessage::markPublished);
        log.debug("Published {} outbox message(s)", batch.size());
    }

    private static ProducerRecord<String, String> toRecord(OutboxMessage message) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(message.getTopic(), message.getMessageKey(), message.getPayload());
        record.headers().add(Topics.HEADER_MESSAGE_ID, message.getId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(Topics.HEADER_MESSAGE_TYPE, message.getMessageType().getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
