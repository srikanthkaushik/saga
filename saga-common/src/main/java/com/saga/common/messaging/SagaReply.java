package com.saga.common.messaging;

import java.util.UUID;

public sealed interface SagaReply
        permits SagaReply.PaymentProcessed, SagaReply.PaymentFailed, SagaReply.PaymentRefunded,
                SagaReply.InventoryReserved, SagaReply.InventoryFailed, SagaReply.InventoryReleased {

    UUID sagaId();

    UUID orderId();

    record PaymentProcessed(UUID sagaId, UUID orderId) implements SagaReply {
    }

    record PaymentFailed(UUID sagaId, UUID orderId, String reason) implements SagaReply {
    }

    record PaymentRefunded(UUID sagaId, UUID orderId) implements SagaReply {
    }

    record InventoryReserved(UUID sagaId, UUID orderId) implements SagaReply {
    }

    record InventoryFailed(UUID sagaId, UUID orderId, String reason) implements SagaReply {
    }

    record InventoryReleased(UUID sagaId, UUID orderId) implements SagaReply {
    }
}
