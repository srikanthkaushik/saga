package com.saga.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "product")
public class Product {

    @Id
    @Column(name = "product_id")
    private String productId;

    @Column(name = "available_quantity", nullable = false)
    private int availableQuantity;

    protected Product() {
    }

    public boolean hasStock(int quantity) {
        return availableQuantity >= quantity;
    }

    public void reserve(int quantity) {
        if (!hasStock(quantity)) {
            throw new IllegalStateException("Insufficient stock for " + productId);
        }
        this.availableQuantity -= quantity;
    }

    public void release(int quantity) {
        this.availableQuantity += quantity;
    }

    public String getProductId() {
        return productId;
    }

    public int getAvailableQuantity() {
        return availableQuantity;
    }
}
