package com.saga.order.saga;

public enum SagaState {
    PAYMENT_PENDING,
    INVENTORY_PENDING,
    /** The pivot step: a scheduled shipment completes the saga; a refused one is compensated. */
    SHIPPING_PENDING,
    /** Shipping step timed out: cancelling any shipment that may have been scheduled, then releasing, then refunding. */
    CANCELLING_SHIPMENT,
    /** Releasing the stock reservation (inventory timed out, or shipping failed), then refunding. */
    RELEASING_INVENTORY,
    /** Refunding the payment. */
    COMPENSATING,
    COMPLETED,
    FAILED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED;
    }

    public boolean isCompensating() {
        return this == CANCELLING_SHIPMENT || this == RELEASING_INVENTORY || this == COMPENSATING;
    }
}
