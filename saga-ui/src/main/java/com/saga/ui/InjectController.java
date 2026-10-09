package com.saga.ui;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code POST /api/inject} - publishes raw records straight onto a saga topic, bypassing every outbox, in the
 * services' wire format ({@code messageId} + {@code messageType} headers, JSON-or-anything value). For demoing
 * poison messages, dead-lettering, replay and fencing. Topics are restricted to the three saga topics.
 */
@RestController
public class InjectController {

    /** Duplicated from saga-common's Topics on purpose: saga-ui does not depend on saga-common. */
    static final Set<String> TOPICS = Set.of("payment.commands", "inventory.commands", "order.saga.replies");
    private static final int MAX_COUNT = 100;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public InjectController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/api/inject")
    public ResponseEntity<Map<String, Object>> inject(@RequestBody InjectRequest request) throws Exception {
        if (request.topic() == null || !TOPICS.contains(request.topic())) {
            return badRequest("topic must be one of " + TOPICS);
        }
        if (request.messageType() == null || request.messageType().isBlank()) {
            return badRequest("messageType is required");
        }
        if (request.payload() == null) {
            return badRequest("payload is required (any string; it does not have to be valid JSON)");
        }
        int count = request.count() == null ? 1 : request.count();
        if (count < 1 || count > MAX_COUNT) {
            return badRequest("count must be between 1 and " + MAX_COUNT);
        }

        List<String> messageIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            UUID messageId = UUID.randomUUID();
            String key = request.key() == null || request.key().isBlank() ? UUID.randomUUID().toString() : request.key();
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(request.topic(), request.partition(), key, request.payload());
            record.headers().add("messageId", messageId.toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add("messageType", request.messageType().strip().getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
            messageIds.add(messageId.toString());
        }
        return ResponseEntity.ok(Map.of("topic", request.topic(), "messageIds", messageIds));
    }

    private static ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }

    public record InjectRequest(String topic, Integer partition, String key, String messageType, String payload,
                                Integer count) {
    }
}
