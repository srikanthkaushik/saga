package com.saga.payment.domain;

public enum PaymentStatus {
    COMPLETED,
    REFUNDED,
    /** Tombstone: a refund arrived before any charge. Fences off a late ProcessPayment for the same order. */
    CANCELLED
}
