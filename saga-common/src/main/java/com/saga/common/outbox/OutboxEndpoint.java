package com.saga.common.outbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * {@code GET /actuator/outbox/{key}} - every message this service emitted for a message key (orderId), oldest
 * first. Merged across services this is the saga's command/reply timeline: order-service holds the commands,
 * the participants hold the replies.
 */
@Component
@Endpoint(id = "outbox")
public class OutboxEndpoint {

    private final OutboxRepository outboxRepository;
    private final JsonMapper jsonMapper;

    public OutboxEndpoint(OutboxRepository outboxRepository, JsonMapper jsonMapper) {
        this.outboxRepository = outboxRepository;
        this.jsonMapper = jsonMapper;
    }

    @ReadOperation
    @Transactional(readOnly = true)
    public List<Entry> messages(@Selector String key) {
        return outboxRepository.findByMessageKeyOrderByCreatedAt(key).stream()
                .map(m -> new Entry(m.getId(), m.getTopic(), m.getMessageType(), jsonMapper.readTree(m.getPayload()),
                        m.getCreatedAt(), m.getPublishedAt()))
                .toList();
    }

    public record Entry(UUID messageId, String topic, String messageType, JsonNode payload,
                        Instant createdAt, Instant publishedAt) {
    }
}
