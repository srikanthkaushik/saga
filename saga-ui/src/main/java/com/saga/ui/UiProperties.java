package com.saga.ui;

import java.net.URI;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param order     base URL of order-service
 * @param payment   base URL of payment-service
 * @param inventory base URL of inventory-service
 */
@ConfigurationProperties("saga.ui.services")
public record UiProperties(
        @DefaultValue("http://localhost:8081") URI order,
        @DefaultValue("http://localhost:8082") URI payment,
        @DefaultValue("http://localhost:8083") URI inventory) {

    /** The only proxy targets; anything else under /api is a 404. */
    public Map<String, URI> targets() {
        return Map.of("order", order, "payment", payment, "inventory", inventory);
    }
}
