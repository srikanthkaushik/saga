package com.saga.common.kafka.dlt;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.saga.common.messaging.Topics;

/**
 * Re-publishes dead-lettered records to their source topic.
 * <p>
 * Progress is tracked as committed offsets of a dedicated consumer group ({@code <app>-dlt-replay}) using
 * manual partition assignment, so "pending" means "not yet replayed" and a replay never re-sends what an
 * earlier replay already sent. Offsets are committed only after every send is acknowledged; a crash in
 * between re-sends at most one batch, which consumers absorb via the unchanged {@code messageId}.
 * <p>
 * Replayed records keep key, value and original headers; {@code kafka_dlt-*} headers are dropped and
 * {@value #REPLAY_COUNT_HEADER} is incremented so repeat offenders are visible on the DLT.
 */
@Component
public class DltReplayer {

    public static final String REPLAY_COUNT_HEADER = "dltReplayCount";

    private static final Logger log = LoggerFactory.getLogger(DltReplayer.class);
    private static final String DLT_HEADER_PREFIX = "kafka_dlt-";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration MAX_REPLAY_DURATION = Duration.ofSeconds(30);

    private final ConsumerFactory<String, String> consumerFactory;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String groupId;
    private final ReentrantLock replayLock = new ReentrantLock();

    public DltReplayer(ConsumerFactory<String, String> consumerFactory,
                       KafkaTemplate<String, String> kafkaTemplate,
                       @Value("${spring.application.name}") String applicationName) {
        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.groupId = applicationName + "-dlt-replay";
    }

    public DltStatus status(String sourceTopic) {
        String dlt = Topics.dlt(sourceTopic);
        try (Consumer<String, String> consumer = newConsumer()) {
            List<TopicPartition> partitions = partitions(consumer, dlt);
            Map<TopicPartition, Long> start = startOffsets(consumer, partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            return new DltStatus(sourceTopic, dlt, pending(start, end));
        }
    }

    public ReplayResult replay(String sourceTopic, int limit) {
        String dlt = Topics.dlt(sourceTopic);
        replayLock.lock();
        try (Consumer<String, String> consumer = newConsumer()) {
            List<TopicPartition> partitions = partitions(consumer, dlt);
            consumer.assign(partitions);
            Map<TopicPartition, Long> next = new HashMap<>(startOffsets(consumer, partitions));
            next.forEach(consumer::seek);
            // Snapshot: records dead-lettered during this replay (including our own re-failures) wait for the next one
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);

            List<CompletableFuture<?>> sends = new ArrayList<>();
            Instant deadline = Instant.now().plus(MAX_REPLAY_DURATION);
            int replayed = 0;
            while (replayed < limit && behind(consumer, partitions, end) && Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> record : consumer.poll(POLL_TIMEOUT)) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    if (replayed >= limit || record.offset() >= end.get(tp)) {
                        continue;
                    }
                    sends.add(kafkaTemplate.send(toSourceRecord(sourceTopic, record)));
                    next.put(tp, record.offset() + 1);
                    replayed++;
                }
            }
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).join();

            Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
            next.forEach((tp, offset) -> commits.put(tp, new OffsetAndMetadata(offset)));
            consumer.commitSync(commits);

            long pending = pending(next, end);
            log.info("Replayed {} record(s) from {} to {}; {} pending", replayed, dlt, sourceTopic, pending);
            return new ReplayResult(sourceTopic, dlt, replayed, pending);
        } finally {
            replayLock.unlock();
        }
    }

    private Consumer<String, String> newConsumer() {
        Properties overrides = new Properties();
        overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerFactory.createConsumer(groupId, null, "-dlt-replay", overrides);
    }

    private static List<TopicPartition> partitions(Consumer<String, String> consumer, String topic) {
        return consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();
    }

    /** Committed replay offset, or the log start if nothing was replayed yet or retention deleted past it. */
    private static Map<TopicPartition, Long> startOffsets(Consumer<String, String> consumer, List<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(partitions));
        Map<TopicPartition, Long> beginning = consumer.beginningOffsets(partitions);
        Map<TopicPartition, Long> start = new HashMap<>();
        for (TopicPartition tp : partitions) {
            OffsetAndMetadata c = committed.get(tp);
            start.put(tp, Math.max(beginning.get(tp), c == null ? 0L : c.offset()));
        }
        return start;
    }

    private static boolean behind(Consumer<String, String> consumer, List<TopicPartition> partitions, Map<TopicPartition, Long> end) {
        return partitions.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp));
    }

    private static long pending(Map<TopicPartition, Long> from, Map<TopicPartition, Long> end) {
        return end.entrySet().stream()
                .mapToLong(e -> Math.max(0, e.getValue() - from.getOrDefault(e.getKey(), 0L)))
                .sum();
    }

    private static ProducerRecord<String, String> toSourceRecord(String sourceTopic, ConsumerRecord<String, String> dead) {
        Headers headers = new RecordHeaders();
        int replayCount = 0;
        for (Header header : dead.headers()) {
            if (header.key().equals(REPLAY_COUNT_HEADER)) {
                replayCount = Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
            } else if (!header.key().startsWith(DLT_HEADER_PREFIX)) {
                headers.add(header);
            }
        }
        headers.add(REPLAY_COUNT_HEADER, String.valueOf(replayCount + 1).getBytes(StandardCharsets.UTF_8));
        return new ProducerRecord<>(sourceTopic, null, dead.key(), dead.value(), headers);
    }
}
