package com.saga.e2e;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Raw Kafka access for tests: injects hand-crafted messages (bypassing the outbox) and waits for a
 * specific message, identified by its messageId header, to land on a dead-letter topic.
 */
final class KafkaProbe implements AutoCloseable {

    private final String bootstrapServers;
    private final KafkaProducer<String, String> producer;

    KafkaProbe(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }

    /** Sends a message in the services' wire format and returns its messageId. */
    UUID send(String topic, String key, String messageType, String json) throws Exception {
        return send(topic, null, key, messageType, json);
    }

    /** As {@link #send(String, String, String, String)} but to an explicit partition (null = by key). */
    UUID send(String topic, Integer partition, String key, String messageType, String json) throws Exception {
        UUID messageId = UUID.randomUUID();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key, json);
        record.headers().add("messageId", messageId.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("messageType", messageType.getBytes(StandardCharsets.UTF_8));
        producer.send(record).get();
        return messageId;
    }

    Optional<ConsumerRecord<String, String>> awaitDeadLetter(String dltTopic, UUID messageId, Duration timeout) {
        return awaitDeadLetter(dltTopic, messageId, timeout, record -> true);
    }

    Optional<ConsumerRecord<String, String>> awaitDeadLetter(String dltTopic, UUID messageId, Duration timeout,
                                                             Predicate<ConsumerRecord<String, String>> matching) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "e2e-probe-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(dltTopic));
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    if (messageId.toString().equals(header(record, "messageId")) && matching.test(record)) {
                        return Optional.of(record);
                    }
                }
            }
            return Optional.empty();
        }
    }

    static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        producer.close();
    }
}
