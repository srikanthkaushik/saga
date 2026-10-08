package com.saga.order.saga;

public enum SagaState {
    PAYMENT_PENDING,
    INVENTORY_PENDING,
    /** Inventory step timed out: releasing any reservation that may have been made, then refunding. */
    RELEASING_INVENTORY,
    /** Refunding the payment. */
    COMPENSATING,
    COMPLETED,
    FAILED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED;
    }

    public boolean isCompensating() {
        return this == RELEASING_INVENTORY || this == COMPENSATING;
    }
}
