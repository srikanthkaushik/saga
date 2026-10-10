package com.saga.shipping.domain;

import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "shipment")
public class Shipment {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false, length = 2)
    private String destination;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ShipmentStatus status;

    @Column(name = "tracking_number")
    private String trackingNumber;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "cancelled_at")
    private Instant cancelledAt;

    protected Shipment() {
    }

    private Shipment(UUID orderId, String productId, int quantity, String destination, ShipmentStatus status,
                     String trackingNumber) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.destination = destination;
        this.status = status;
        this.trackingNumber = trackingNumber;
        this.createdAt = Instant.now();
        if (status == ShipmentStatus.CANCELLED) {
            this.cancelledAt = this.createdAt;
        }
    }

    public static Shipment scheduled(UUID orderId, String productId, int quantity, String destination) {
        String tracking = "TRK-" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase(Locale.ROOT);
        return new Shipment(orderId, productId, quantity, destination, ShipmentStatus.SCHEDULED, tracking);
    }

    /**
     * Cancel arrived before any schedule: record it so a late schedule for this order is refused. CancelShipment
     * carries no line details, so the tombstone holds placeholders.
     */
    public static Shipment tombstone(UUID orderId) {
        return new Shipment(orderId, "-", 1, "--", ShipmentStatus.CANCELLED, null);
    }

    public void cancel() {
        if (status != ShipmentStatus.SCHEDULED) {
            throw new IllegalStateException("Shipment " + id + " is " + status);
        }
        this.status = ShipmentStatus.CANCELLED;
        this.cancelledAt = Instant.now();
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

    public String getDestination() {
        return destination;
    }

    public ShipmentStatus getStatus() {
        return status;
    }

    public String getTrackingNumber() {
        return trackingNumber;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getCancelledAt() {
        return cancelledAt;
    }
}
