package com.saga.shipping.domain;

public enum ShipmentStatus {
    SCHEDULED,
    /** Shipment cancelled, or (tombstone) cancelled before anything was scheduled. Fences off a late schedule. */
    CANCELLED
}
