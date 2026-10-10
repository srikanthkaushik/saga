package com.saga.order.api;

import java.math.BigDecimal;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record CreateOrderRequest(
        @NotBlank String customerId,
        @NotBlank String productId,
        @Min(1) int quantity,
        @NotNull @DecimalMin(value = "0.01") @Digits(integer = 17, fraction = 2) BigDecimal amount,
        @Pattern(regexp = "[A-Z]{2}", message = "must be an ISO country code such as GB") String destination) {

    public static final String DEFAULT_DESTINATION = "GB";

    /** Destination is optional so existing clients keep working; they ship to the default country. */
    public CreateOrderRequest {
        if (destination == null || destination.isBlank()) {
            destination = DEFAULT_DESTINATION;
        }
    }
}
