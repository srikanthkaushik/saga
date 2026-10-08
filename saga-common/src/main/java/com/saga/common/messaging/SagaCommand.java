package com.saga.common.messaging;

import java.math.BigDecimal;
import java.util.UUID;

public sealed interface SagaCommand
        permits SagaCommand.ProcessPayment, SagaCommand.RefundPayment,
                SagaCommand.ReserveInventory, SagaCommand.ReleaseInventory {

    UUID sagaId();

    UUID orderId();

    record ProcessPayment(UUID sagaId, UUID orderId, String customerId, BigDecimal amount) implements SagaCommand {
    }

    record RefundPayment(UUID sagaId, UUID orderId) implements SagaCommand {
    }

    record ReserveInventory(UUID sagaId, UUID orderId, String productId, int quantity) implements SagaCommand {
    }

    record ReleaseInventory(UUID sagaId, UUID orderId, String productId, int quantity) implements SagaCommand {
    }
}
