package com.saga.order.saga;

import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.saga.common.idempotency.IdempotencyGuard;
import com.saga.common.messaging.InboundMessage;
import com.saga.common.messaging.MessageCodec;
import com.saga.common.messaging.NonRetryableMessageException;
import com.saga.common.messaging.SagaCommand.RefundPayment;
import com.saga.common.messaging.SagaCommand.ReleaseInventory;
import com.saga.common.messaging.SagaCommand.ReserveInventory;
import com.saga.common.messaging.SagaReply;
import com.saga.common.messaging.SagaReply.InventoryFailed;
import com.saga.common.messaging.SagaReply.InventoryReleased;
import com.saga.common.messaging.SagaReply.InventoryReserved;
import com.saga.common.messaging.SagaReply.PaymentFailed;
import com.saga.common.messaging.SagaReply.PaymentProcessed;
import com.saga.common.messaging.SagaReply.PaymentRefunded;
import com.saga.common.messaging.Topics;
import com.saga.common.outbox.OutboxWriter;
import com.saga.order.domain.Order;
import com.saga.order.domain.OrderRepository;

/**
 * Drives the order saga:
 * <pre>
 * PAYMENT_PENDING --PaymentProcessed--> INVENTORY_PENDING --InventoryReserved--> COMPLETED
 *       |  PaymentFailed -> FAILED            |  InventoryFailed -> COMPENSATING
 *       |  timeout -> COMPENSATING            |  timeout -> RELEASING_INVENTORY --InventoryReleased--> COMPENSATING
 * COMPENSATING --PaymentRefunded--> FAILED
 * RELEASING_INVENTORY / COMPENSATING timeout -> re-send the compensation command, wait again
 * </pre>
 * Each reply or timeout is handled in one DB transaction covering the state change and the next command
 * (via the outbox). Timeouts compensate unconditionally: a slow participant's late charge or reservation is
 * undone because the compensation command shares its key (orderId) and therefore its partition, so it is
 * always processed after the original command. The late reply itself is dropped by the state guard.
 */
@Component
public class OrderSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaOrchestrator.class);

    private final MessageCodec codec;
    private final IdempotencyGuard idempotencyGuard;
    private final OrderSagaRepository sagaRepository;
    private final OrderRepository orderRepository;
    private final OutboxWriter outbox;
    private final SagaTimeoutProperties timeouts;
    private final CompensationMonitor compensationMonitor;

    public OrderSagaOrchestrator(MessageCodec codec,
                                 IdempotencyGuard idempotencyGuard,
                                 OrderSagaRepository sagaRepository,
                                 OrderRepository orderRepository,
                                 OutboxWriter outbox,
                                 SagaTimeoutProperties timeouts,
                                 CompensationMonitor compensationMonitor) {
        this.codec = codec;
        this.idempotencyGuard = idempotencyGuard;
        this.sagaRepository = sagaRepository;
        this.orderRepository = orderRepository;
        this.outbox = outbox;
        this.timeouts = timeouts;
        this.compensationMonitor = compensationMonitor;
    }

    @KafkaListener(topics = Topics.SAGA_REPLIES)
    @Transactional
    public void onReply(ConsumerRecord<String, String> record) {
        InboundMessage inbound = codec.decode(record);
        if (!idempotencyGuard.firstDelivery(inbound.messageId())) {
            log.debug("Duplicate reply {} ignored", inbound.messageId());
            return;
        }
        if (!(inbound.payload() instanceof SagaReply reply)) {
            throw new NonRetryableMessageException(
                    "Non-reply message " + inbound.payload().getClass().getSimpleName() + " on " + record.topic());
        }

        // The saga row commits before its first command leaves the outbox, so a missing saga is permanent.
        OrderSaga saga = sagaRepository.findById(reply.sagaId())
                .orElseThrow(() -> new NonRetryableMessageException("Unknown saga " + reply.sagaId()));
        SagaState expected = expectedState(reply);
        if (saga.getState() != expected) {
            log.warn("Saga {} in state {} ignoring {} (expected state {})",
                    saga.getId(), saga.getState(), reply.getClass().getSimpleName(), expected);
            return;
        }

        Order order = orderOf(saga);
        Instant now = Instant.now();
        switch (reply) {
            case PaymentProcessed p -> {
                saga.paymentCompleted(now.plus(timeouts.inventory()));
                sendReserveInventory(saga, order);
            }
            case PaymentFailed f -> {
                saga.paymentFailed(f.reason());
                order.reject(f.reason());
            }
            case InventoryReserved r -> {
                saga.inventoryReserved();
                order.approve();
            }
            case InventoryFailed f -> {
                saga.inventoryFailed(f.reason(), now.plus(timeouts.compensation()));
                sendRefundPayment(saga, order);
            }
            case InventoryReleased r -> {
                saga.inventoryReleased(now.plus(timeouts.compensation()));
                sendRefundPayment(saga, order);
            }
            case PaymentRefunded r -> {
                saga.compensated();
                order.reject(saga.getFailureReason());
            }
        }
        log.info("Saga {} for order {} -> {}", saga.getId(), order.getId(), saga.getState());
    }

    /**
     * Called by {@link SagaTimeoutScanner} inside its own transaction. Re-checks the deadline because the
     * saga may have advanced between the scan query and this call.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void onTimeout(UUID sagaId, Instant now) {
        OrderSaga saga = sagaRepository.findById(sagaId).orElse(null);
        if (saga == null || !saga.isOverdue(now)) {
            return;
        }
        Order order = orderOf(saga);
        Instant compensationDeadline = now.plus(timeouts.compensation());
        switch (saga.getState()) {
            case PAYMENT_PENDING -> {
                saga.paymentTimedOut(compensationDeadline);
                sendRefundPayment(saga, order);
            }
            case INVENTORY_PENDING -> {
                saga.inventoryTimedOut(compensationDeadline);
                sendReleaseInventory(saga, order);
            }
            case RELEASING_INVENTORY, COMPENSATING -> {
                reportAfterCommit(saga, saga.resendCompensation(compensationDeadline));
                sendCompensation(saga, order);
            }
            case COMPLETED, FAILED -> {
                return;
            }
        }
        log.warn("Saga {} for order {} timed out -> {}", saga.getId(), order.getId(), saga.getState());
    }

    /** Only count and log the re-send if it actually commits (it may lose an optimistic-lock race). */
    private void reportAfterCommit(OrderSaga saga, int resendCount) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                compensationMonitor.compensationResent(saga, resendCount);
            }
        });
    }

    /** Sends the command for the saga's current compensation step (release inventory, or refund payment). */
    public void sendCompensation(OrderSaga saga, Order order) {
        switch (saga.getState()) {
            case RELEASING_INVENTORY -> sendReleaseInventory(saga, order);
            case COMPENSATING -> sendRefundPayment(saga, order);
            default -> throw new IllegalStateException("Saga " + saga.getId() + " in " + saga.getState() + " is not compensating");
        }
    }

    public Order orderOf(OrderSaga saga) {
        return orderRepository.findById(saga.getOrderId())
                .orElseThrow(() -> new IllegalStateException("Saga " + saga.getId() + " has no order " + saga.getOrderId()));
    }

    private void sendReserveInventory(OrderSaga saga, Order order) {
        outbox.write(Topics.INVENTORY_COMMANDS, order.getId().toString(),
                new ReserveInventory(saga.getId(), order.getId(), order.getProductId(), order.getQuantity()));
    }

    private void sendReleaseInventory(OrderSaga saga, Order order) {
        outbox.write(Topics.INVENTORY_COMMANDS, order.getId().toString(),
                new ReleaseInventory(saga.getId(), order.getId(), order.getProductId(), order.getQuantity()));
    }

    private void sendRefundPayment(OrderSaga saga, Order order) {
        outbox.write(Topics.PAYMENT_COMMANDS, order.getId().toString(), new RefundPayment(saga.getId(), order.getId()));
    }

    private static SagaState expectedState(SagaReply reply) {
        return switch (reply) {
            case PaymentProcessed p -> SagaState.PAYMENT_PENDING;
            case PaymentFailed f -> SagaState.PAYMENT_PENDING;
            case InventoryReserved r -> SagaState.INVENTORY_PENDING;
            case InventoryFailed f -> SagaState.INVENTORY_PENDING;
            case InventoryReleased r -> SagaState.RELEASING_INVENTORY;
            case PaymentRefunded r -> SagaState.COMPENSATING;
        };
    }
}
