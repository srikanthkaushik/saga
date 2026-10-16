package com.saga.order.saga;

import java.time.Instant;
import java.util.UUID;

/** Builds a saga in any state by walking it there through legal transitions only. */
final class OrderSagas {

    private OrderSagas() {
    }

    static OrderSaga in(SagaState state) {
        return in(state, UUID.randomUUID(), Instant.now().plusSeconds(30));
    }

    /** @param deadline the deadline of the final state (ignored for terminal states, which have none) */
    static OrderSaga in(SagaState state, UUID orderId, Instant deadline) {
        OrderSaga saga = new OrderSaga(orderId, deadline);
        switch (state) {
            case PAYMENT_PENDING -> {
            }
            case INVENTORY_PENDING -> saga.paymentCompleted(deadline);
            case SHIPPING_PENDING -> {
                saga.paymentCompleted(deadline);
                saga.inventoryReserved(deadline);
            }
            case CANCELLING_SHIPMENT -> {
                saga.paymentCompleted(deadline);
                saga.inventoryReserved(deadline);
                saga.shippingTimedOut(deadline);
            }
            case RELEASING_INVENTORY -> {
                saga.paymentCompleted(deadline);
                saga.inventoryReserved(deadline);
                saga.shipmentFailed("No carrier serves XX", deadline);
            }
            case COMPENSATING -> {
                saga.paymentCompleted(deadline);
                saga.inventoryFailed("Insufficient stock", deadline);
            }
            case COMPLETED -> {
                saga.paymentCompleted(deadline);
                saga.inventoryReserved(deadline);
                saga.shipmentScheduled();
            }
            case FAILED -> saga.paymentFailed("Insufficient credit");
        }
        return saga;
    }
}
