package com.saga.common.test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.saga.common.messaging.MessageCodec;
import com.saga.common.messaging.Topics;
import com.saga.common.outbox.OutboxMessage;
import com.saga.common.outbox.OutboxRepository;

/** Builds inbound records the way the outbox relay publishes them, and reads back what a handler sent. */
public final class TestMessages {

    private TestMessages() {
    }

    /** A record as delivered to a listener: JSON value plus the messageId and messageType headers. */
    public static ConsumerRecord<String, String> record(MessageCodec codec, String topic, UUID messageId, Object message) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(topic, 0, 0L, null, codec.serialize(message));
        record.headers().add(Topics.HEADER_MESSAGE_ID, messageId.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(Topics.HEADER_MESSAGE_TYPE, codec.typeName(message).getBytes(StandardCharsets.UTF_8));
        return record;
    }

    /** Messages written to the outbox for {@code orderId} on {@code topic}, decoded, oldest first. */
    public static List<Object> sent(OutboxRepository outbox, MessageCodec codec, String topic, UUID orderId) {
        return outbox.findByMessageKeyOrderByCreatedAt(orderId.toString()).stream()
                .filter(message -> message.getTopic().equals(topic))
                .map(message -> codec.decode(asRecord(message)).payload())
                .toList();
    }

    private static ConsumerRecord<String, String> asRecord(OutboxMessage message) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                message.getTopic(), 0, 0L, message.getMessageKey(), message.getPayload());
        record.headers().add(Topics.HEADER_MESSAGE_ID, message.getId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(Topics.HEADER_MESSAGE_TYPE, message.getMessageType().getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
