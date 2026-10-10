package com.saga.shipping.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "shipping_zone")
public class ShippingZone {

    @Id
    @Column(length = 2)
    private String country;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private boolean served;

    protected ShippingZone() {
    }

    public ShippingZone(String country, String name, boolean served) {
        this.country = country;
        this.name = name;
        this.served = served;
    }

    public String getCountry() {
        return country;
    }

    public String getName() {
        return name;
    }

    public boolean isServed() {
        return served;
    }

    /** Admin/test tooling: open or close a destination. */
    public void setServed(boolean served) {
        this.served = served;
    }

    public void setName(String name) {
        this.name = name;
    }
}
