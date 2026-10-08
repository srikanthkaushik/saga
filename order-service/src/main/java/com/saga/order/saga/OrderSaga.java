package com.saga.order.saga;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * Persistent saga state. Transitions are guarded so an out-of-order or stale reply can never move the
 * saga backwards; callers check {@link #getState()} before invoking a transition.
 * <p>
 * Every non-terminal state carries a {@link #getDeadline() deadline}; {@code SagaTimeoutScanner} acts on
 * sagas past it. Terminal states clear it. {@code @Version} makes a reply and a timeout racing on the same
 * saga mutually exclusive: the loser gets an optimistic-lock failure and re-evaluates.
 */
@Entity
@Table(name = "order_saga")
public class OrderSaga {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SagaState state;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column
    private Instant deadline;

    /** When the saga first entered compensation (RELEASING_INVENTORY or COMPENSATING); null if it never did. */
    @Column(name = "compensating_since")
    private Instant compensatingSince;

    /** Re-sends of the current compensation command after it timed out; reset on entering a new compensation step. */
    @Column(name = "compensation_resends", nullable = false)
    private int compensationResends;

    @Version
    private long version;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected OrderSaga() {
    }

    public OrderSaga(UUID orderId, Instant paymentDeadline) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.state = SagaState.PAYMENT_PENDING;
        this.deadline = paymentDeadline;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public void paymentCompleted(Instant inventoryDeadline) {
        transition(SagaState.PAYMENT_PENDING, SagaState.INVENTORY_PENDING, inventoryDeadline);
    }

    public void paymentFailed(String reason) {
        transition(SagaState.PAYMENT_PENDING, SagaState.FAILED, null);
        this.failureReason = reason;
    }

    public void paymentTimedOut(Instant compensationDeadline) {
        transition(SagaState.PAYMENT_PENDING, SagaState.COMPENSATING, compensationDeadline);
        this.failureReason = "Payment timed out";
    }

    public void inventoryReserved() {
        transition(SagaState.INVENTORY_PENDING, SagaState.COMPLETED, null);
    }

    public void inventoryFailed(String reason, Instant compensationDeadline) {
        transition(SagaState.INVENTORY_PENDING, SagaState.COMPENSATING, compensationDeadline);
        this.failureReason = reason;
    }

    public void inventoryTimedOut(Instant compensationDeadline) {
        transition(SagaState.INVENTORY_PENDING, SagaState.RELEASING_INVENTORY, compensationDeadline);
        this.failureReason = "Inventory timed out";
    }

    public void inventoryReleased(Instant compensationDeadline) {
        transition(SagaState.RELEASING_INVENTORY, SagaState.COMPENSATING, compensationDeadline);
    }

    public void compensated() {
        transition(SagaState.COMPENSATING, SagaState.FAILED, null);
    }

    /**
     * A compensation step timed out: stay in the same state, re-send, and wait again.
     *
     * @return the number of re-sends of the current compensation step so far
     */
    public int resendCompensation(Instant newDeadline) {
        requireCompensating();
        this.deadline = newDeadline;
        this.compensationResends++;
        this.updatedAt = Instant.now();
        return compensationResends;
    }

    /** Operator retry: re-send the current compensation now and restart the stuck count. */
    public void retryCompensationManually(Instant newDeadline) {
        requireCompensating();
        this.deadline = newDeadline;
        this.compensationResends = 0;
        this.updatedAt = Instant.now();
    }

    /** Operator confirmed the compensation was completed outside the saga: close it as FAILED. */
    public void resolveManually() {
        requireCompensating();
        this.state = SagaState.FAILED;
        this.deadline = null;
        this.updatedAt = Instant.now();
    }

    private void requireCompensating() {
        if (!state.isCompensating()) {
            throw new IllegalStateException("Saga " + id + " in " + state + " is not compensating");
        }
    }

    public boolean isOverdue(Instant now) {
        return deadline != null && !deadline.isAfter(now);
    }

    private void transition(SagaState expected, SagaState next, Instant nextDeadline) {
        if (state != expected) {
            throw new IllegalStateException(
                    "Saga " + id + " cannot move " + state + " -> " + next + " (expected " + expected + ")");
        }
        this.state = next;
        this.deadline = next.isTerminal() ? null : nextDeadline;
        this.updatedAt = Instant.now();
        if (next.isCompensating()) {
            this.compensationResends = 0;
            if (compensatingSince == null) {
                this.compensatingSince = this.updatedAt;
            }
        }
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public SagaState getState() {
        return state;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public Instant getDeadline() {
        return deadline;
    }

    public Instant getCompensatingSince() {
        return compensatingSince;
    }

    public int getCompensationResends() {
        return compensationResends;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
