package com.example.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "inventory")
public class Inventory {

    @Id
    private String productId;

    @Column(nullable = false)
    private int available;

    @Version
    private long version; // optimistic lock: concurrent reservations cannot oversell

    protected Inventory() {
    }

    public Inventory(String productId, int available) {
        this.productId = productId;
        this.available = available;
    }

    public boolean tryReserve(int quantity) {
        if (available < quantity) {
            return false;
        }
        available -= quantity;
        return true;
    }

    public String getProductId() {
        return productId;
    }

    public int getAvailable() {
        return available;
    }
}
