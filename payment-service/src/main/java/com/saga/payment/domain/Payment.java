package com.saga.payment.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "payment")
public class Payment {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    /** Null for {@link PaymentStatus#CANCELLED} tombstones. */
    @Column(name = "customer_id")
    private String customerId;

    /** Null for {@link PaymentStatus#CANCELLED} tombstones. */
    @Column(precision = 19, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Payment() {
    }

    public Payment(UUID orderId, String customerId, BigDecimal amount) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = PaymentStatus.COMPLETED;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public static Payment cancelled(UUID orderId) {
        Payment payment = new Payment();
        payment.id = UUID.randomUUID();
        payment.orderId = orderId;
        payment.status = PaymentStatus.CANCELLED;
        payment.createdAt = Instant.now();
        payment.updatedAt = payment.createdAt;
        return payment;
    }

    public void refund() {
        if (status != PaymentStatus.COMPLETED) {
            throw new IllegalStateException("Payment " + id + " is " + status);
        }
        this.status = PaymentStatus.REFUNDED;
        this.updatedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public PaymentStatus getStatus() {
        return status;
    }
}
