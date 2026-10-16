package com.saga.common.test;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.saga.common.idempotency.IdempotencyGuard;
import com.saga.common.messaging.MessageCodec;
import com.saga.common.outbox.OutboxWriter;

import tools.jackson.databind.json.JsonMapper;

@TestConfiguration(proxyBeanMethods = false)
@Import({MessageCodec.class, IdempotencyGuard.class, OutboxWriter.class})
public class SliceTestConfig {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:17");
    }

    /** The JPA slice has no Jackson auto-configuration; the codec only needs a default mapper. */
    @Bean
    JsonMapper jsonMapper() {
        return JsonMapper.builder().build();
    }
}
