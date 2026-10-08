package com.saga.inventory.domain;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "reservation")
public class Reservation {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(nullable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ReservationStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "released_at")
    private Instant releasedAt;

    protected Reservation() {
    }

    public Reservation(UUID orderId, String productId, int quantity) {
        this(orderId, productId, quantity, ReservationStatus.RESERVED);
    }

    private Reservation(UUID orderId, String productId, int quantity, ReservationStatus status) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.status = status;
        this.createdAt = Instant.now();
        if (status == ReservationStatus.RELEASED) {
            this.releasedAt = this.createdAt;
        }
    }

    /** Release arrived before any reservation: record it so a late reserve for this order is refused. */
    public static Reservation tombstone(UUID orderId, String productId, int quantity) {
        return new Reservation(orderId, productId, quantity, ReservationStatus.RELEASED);
    }

    public void release() {
        if (status != ReservationStatus.RESERVED) {
            throw new IllegalStateException("Reservation " + id + " is " + status);
        }
        this.status = ReservationStatus.RELEASED;
        this.releasedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getReleasedAt() {
        return releasedAt;
    }
}
