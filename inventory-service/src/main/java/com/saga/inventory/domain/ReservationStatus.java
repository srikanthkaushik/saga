package com.saga.inventory.domain;

public enum ReservationStatus {
    RESERVED,
    /** Stock returned, or (tombstone) released before anything was reserved. Fences off a late reserve. */
    RELEASED
}
