package com.saga.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import com.saga.common.messaging.NonRetryableMessageException;
import com.saga.common.messaging.Topics;

import tools.jackson.core.JacksonException;

/**
 * Boot attaches a {@code CommonErrorHandler} bean to the default listener container factory.
 * <p>
 * Failed records are retried in place with exponential backoff (blocking the partition, which keeps
 * per-order ordering intact), then published to {@code <topic>-dlt} on the same partition with the
 * original headers plus {@code kafka_dlt-*} exception headers. Each retry runs in a fresh DB
 * transaction, so the idempotency marker from a failed attempt has already rolled back.
 */
@Configuration
@EnableConfigurationProperties(RetryProperties.class)
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate, RetryProperties retry) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(NonRetryableMessageException.class, JacksonException.class);
        handler.setRetryListeners(new LoggingRetryListener());
        return handler;
    }

    /** Spring Kafka retries and dead-letters silently by default; make both visible. */
    private static final class LoggingRetryListener implements RetryListener {

        @Override
        public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
            log.warn("Delivery attempt {} failed for {}-{}@{}: {}",
                    deliveryAttempt, record.topic(), record.partition(), record.offset(), rootMessage(ex));
        }

        @Override
        public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
            log.error("Dead-lettered {}-{}@{} to {}: {}",
                    record.topic(), record.partition(), record.offset(), Topics.dlt(record.topic()), rootMessage(ex));
        }

        @Override
        public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
            log.error("Could not dead-letter {}-{}@{}; it will be redelivered",
                    record.topic(), record.partition(), record.offset(), failure);
        }

        private static String rootMessage(Throwable ex) {
            Throwable root = ex;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            return root.getClass().getSimpleName() + ": " + root.getMessage();
        }
    }
}
