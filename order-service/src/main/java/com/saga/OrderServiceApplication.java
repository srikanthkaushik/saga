package com.saga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.saga.order.saga.SagaAlertProperties;
import com.saga.order.saga.SagaTimeoutProperties;

/**
 * Lives in the root {@code com.saga} package so component, entity and repository scanning also cover
 * {@code com.saga.common} from the shared module.
 */
@SpringBootApplication
@EnableConfigurationProperties({SagaTimeoutProperties.class, SagaAlertProperties.class})
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
