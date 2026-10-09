package com.saga.ui;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Test/demo console for the saga. Serves the single-page UI from {@code static/}, proxies
 * {@code /api/{order|payment|inventory}/**} to the services (one origin, no CORS on the services) and injects raw
 * Kafka messages for DLT/replay demos. Binds to 127.0.0.1 by default: it is unauthenticated tooling.
 */
@SpringBootApplication
@EnableConfigurationProperties(UiProperties.class)
public class SagaUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(SagaUiApplication.class, args);
    }
}
