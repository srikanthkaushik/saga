package com.saga.common.messaging;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Domain events order-service publishes on {@link Topics#ORDER_EVENTS} when an order reaches its outcome, in the
 * same transaction as the outcome itself (outbox). Unlike commands and replies nobody is waiting for them: any
 * service may subscribe (choreography) without the orchestrator knowing it exists.
 */
public sealed interface OrderEvent permits OrderEvent.OrderApproved, OrderEvent.OrderRejected {

    UUID orderId();

    String customerId();

    String productId();

    int quantity();

    BigDecimal amount();

    String destination();

    record OrderApproved(UUID orderId, String customerId, String productId, int quantity, BigDecimal amount,
                         String destination, String trackingNumber) implements OrderEvent {
    }

    record OrderRejected(UUID orderId, String customerId, String productId, int quantity, BigDecimal amount,
                         String destination, String reason) implements OrderEvent {
    }
}
