package com.saga.common.test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * JPA slice of a service against a real Postgres 17 (Testcontainers), with the service's own Flyway migrations
 * applied and {@code ddl-auto: validate} checking the entities against them. Adds the messaging beans a command
 * or event handler needs ({@link SliceTestConfig}); the test {@code @Import}s the handler itself.
 * <p>
 * No Kafka: tests call the listener method directly with a {@link TestMessages#record record}, and read what the
 * handler sent from the outbox table. Each test runs in a transaction that is rolled back, which the handlers'
 * {@code MANDATORY} collaborators (idempotency guard, outbox writer) join. Tests that need the handler's own
 * transaction to commit or roll back mark themselves {@code @Transactional(propagation = NOT_SUPPORTED)}.
 * <p>
 * Skipped when Docker is not available. One container per module: every test class with the same
 * configuration shares the cached application context and therefore the container.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@DataJpaTest(showSql = false)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(SliceTestConfig.class)
@Testcontainers(disabledWithoutDocker = true)
public @interface PostgresSliceTest {
}
