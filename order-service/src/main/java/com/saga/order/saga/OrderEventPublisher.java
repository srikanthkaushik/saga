package com.saga.order.saga;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.saga.common.messaging.OrderEvent.OrderApproved;
import com.saga.common.messaging.OrderEvent.OrderRejected;
import com.saga.common.messaging.Topics;
import com.saga.common.outbox.OutboxWriter;
import com.saga.order.domain.Order;

/**
 * Publishes the order's outcome as a domain event, through the outbox in the transaction that decides it: the event
 * exists if and only if the order is approved or rejected. Every terminal path - saga replies and operator resolve -
 * goes through here, so subscribers (notification-service) hear about every outcome exactly once.
 */
@Component
public class OrderEventPublisher {

    private final OutboxWriter outbox;

    public OrderEventPublisher(OutboxWriter outbox) {
        this.outbox = outbox;
    }

    /** Approves the order and publishes {@link OrderApproved}. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void approve(Order order, String trackingNumber) {
        order.approve();
        outbox.write(Topics.ORDER_EVENTS, order.getId().toString(), new OrderApproved(order.getId(), order.getCustomerId(),
                order.getProductId(), order.getQuantity(), order.getAmount(), order.getDestination(), trackingNumber));
    }

    /** Rejects the order and publishes {@link OrderRejected}. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void reject(Order order, String reason) {
        order.reject(reason);
        outbox.write(Topics.ORDER_EVENTS, order.getId().toString(), new OrderRejected(order.getId(), order.getCustomerId(),
                order.getProductId(), order.getQuantity(), order.getAmount(), order.getDestination(), reason));
    }
}
