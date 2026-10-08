package com.saga.common.messaging;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Serializes saga messages to JSON and resolves them back by the simple type name carried in the
 * {@code messageType} header. The registry is derived from the sealed hierarchies, so adding a
 * record to {@link SagaCommand} or {@link SagaReply} registers it automatically.
 */
@Component
public class MessageCodec {

    private final JsonMapper jsonMapper;
    private final Map<String, Class<?>> types;

    public MessageCodec(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
        this.types = Stream.concat(
                        Arrays.stream(SagaCommand.class.getPermittedSubclasses()),
                        Arrays.stream(SagaReply.class.getPermittedSubclasses()))
                .collect(Collectors.toUnmodifiableMap(Class::getSimpleName, Function.identity()));
    }

    public String typeName(Object message) {
        String name = message.getClass().getSimpleName();
        if (!types.containsKey(name)) {
            throw new IllegalArgumentException("Unregistered message type: " + message.getClass().getName());
        }
        return name;
    }

    public String serialize(Object message) {
        return jsonMapper.writeValueAsString(message);
    }

    /**
     * @throws NonRetryableMessageException if the record is malformed; redelivery cannot fix it
     */
    public InboundMessage decode(ConsumerRecord<String, String> record) {
        String typeName = requiredHeader(record, Topics.HEADER_MESSAGE_TYPE);
        String rawMessageId = requiredHeader(record, Topics.HEADER_MESSAGE_ID);
        Class<?> type = types.get(typeName);
        if (type == null) {
            throw new NonRetryableMessageException("Unknown message type '" + typeName + "' on " + record.topic());
        }
        try {
            return new InboundMessage(UUID.fromString(rawMessageId), jsonMapper.readValue(record.value(), type));
        } catch (IllegalArgumentException | JacksonException e) {
            throw new NonRetryableMessageException("Undecodable " + typeName + " on " + record.topic(), e);
        }
    }

    private static String requiredHeader(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            throw new NonRetryableMessageException("Missing header '" + name + "' on " + record.topic());
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
