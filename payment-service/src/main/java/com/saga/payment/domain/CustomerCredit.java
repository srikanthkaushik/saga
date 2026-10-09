package com.saga.payment.domain;

import java.math.BigDecimal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "customer_credit")
public class CustomerCredit {

    @Id
    @Column(name = "customer_id")
    private String customerId;

    @Column(name = "available_credit", nullable = false, precision = 19, scale = 2)
    private BigDecimal availableCredit;

    protected CustomerCredit() {
    }

    public CustomerCredit(String customerId, BigDecimal availableCredit) {
        this.customerId = customerId;
        setAvailableCredit(availableCredit);
    }

    /** Admin/test seeding only; saga processing goes through {@link #debit} and {@link #credit}. */
    public void setAvailableCredit(BigDecimal availableCredit) {
        if (availableCredit.signum() < 0) {
            throw new IllegalArgumentException("Credit cannot be negative");
        }
        this.availableCredit = availableCredit;
    }

    public boolean canAfford(BigDecimal amount) {
        return availableCredit.compareTo(amount) >= 0;
    }

    public void debit(BigDecimal amount) {
        if (!canAfford(amount)) {
            throw new IllegalStateException("Insufficient credit for " + customerId);
        }
        this.availableCredit = availableCredit.subtract(amount);
    }

    public void credit(BigDecimal amount) {
        this.availableCredit = availableCredit.add(amount);
    }

    public String getCustomerId() {
        return customerId;
    }

    public BigDecimal getAvailableCredit() {
        return availableCredit;
    }
}
