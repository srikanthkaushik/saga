package com.saga.order.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import com.saga.order.domain.Order;
import com.saga.order.domain.OrderStatus;
import com.saga.order.saga.OrderSaga;
import com.saga.order.saga.SagaState;

public record OrderResponse(
        UUID id,
        String customerId,
        String productId,
        int quantity,
        BigDecimal amount,
        OrderStatus status,
        String rejectionReason,
        UUID sagaId,
        SagaState sagaState,
        Instant createdAt,
        Instant updatedAt) {

    public static OrderResponse of(Order order, OrderSaga saga) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getProductId(),
                order.getQuantity(),
                order.getAmount(),
                order.getStatus(),
                order.getRejectionReason(),
                saga.getId(),
                saga.getState(),
                order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
