package com.example.order.web;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;

public record CreateOrderRequest(
        @NotBlank String customerId,
        @NotBlank String productId,
        @Min(1) int quantity,
        @NotNull @Positive BigDecimal amount
) {
}
