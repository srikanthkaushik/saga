package com.saga.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.saga.common.messaging.Topics;

@Configuration
@EnableScheduling
public class SagaCommonConfig {

    /** DLTs must have at least as many partitions as their source: records keep their partition. */
    private static final int PARTITIONS = 3;
    private static final int REPLICAS = 1;
    private static final Duration DLT_RETENTION = Duration.ofDays(14);

    @Bean
    KafkaAdmin.NewTopics sagaTopics() {
        List<NewTopic> topics = new ArrayList<>();
        for (String topic : Topics.ALL) {
            topics.add(TopicBuilder.name(topic).partitions(PARTITIONS).replicas(REPLICAS).build());
            topics.add(TopicBuilder.name(Topics.dlt(topic))
                    .partitions(PARTITIONS)
                    .replicas(REPLICAS)
                    .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(DLT_RETENTION.toMillis()))
                    .build());
        }
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }
}
