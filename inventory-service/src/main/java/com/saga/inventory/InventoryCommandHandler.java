package com.saga.inventory;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.saga.common.idempotency.IdempotencyGuard;
import com.saga.common.messaging.InboundMessage;
import com.saga.common.messaging.MessageCodec;
import com.saga.common.messaging.NonRetryableMessageException;
import com.saga.common.messaging.SagaCommand.ReleaseInventory;
import com.saga.common.messaging.SagaCommand.ReserveInventory;
import com.saga.common.messaging.SagaReply;
import com.saga.common.messaging.SagaReply.InventoryFailed;
import com.saga.common.messaging.SagaReply.InventoryReleased;
import com.saga.common.messaging.SagaReply.InventoryReserved;
import com.saga.common.messaging.Topics;
import com.saga.common.outbox.OutboxWriter;
import com.saga.inventory.domain.Product;
import com.saga.inventory.domain.ProductRepository;
import com.saga.inventory.domain.Reservation;
import com.saga.inventory.domain.ReservationRepository;
import com.saga.inventory.domain.ReservationStatus;

@Component
public class InventoryCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandHandler.class);

    private final MessageCodec codec;
    private final IdempotencyGuard idempotencyGuard;
    private final ProductRepository productRepository;
    private final ReservationRepository reservationRepository;
    private final OutboxWriter outbox;

    public InventoryCommandHandler(MessageCodec codec,
                                   IdempotencyGuard idempotencyGuard,
                                   ProductRepository productRepository,
                                   ReservationRepository reservationRepository,
                                   OutboxWriter outbox) {
        this.codec = codec;
        this.idempotencyGuard = idempotencyGuard;
        this.productRepository = productRepository;
        this.reservationRepository = reservationRepository;
        this.outbox = outbox;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS)
    @Transactional
    public void onCommand(ConsumerRecord<String, String> record) {
        InboundMessage inbound = codec.decode(record);
        if (!idempotencyGuard.firstDelivery(inbound.messageId())) {
            log.debug("Duplicate command {} ignored", inbound.messageId());
            return;
        }

        SagaReply reply = switch (inbound.payload()) {
            case ReserveInventory cmd -> reserve(cmd);
            case ReleaseInventory cmd -> release(cmd);
            default -> throw new NonRetryableMessageException(
                    "Unsupported message on " + record.topic() + ": " + inbound.payload().getClass().getSimpleName());
        };
        outbox.write(Topics.SAGA_REPLIES, reply.orderId().toString(), reply);
        log.info("Order {} -> {}", reply.orderId(), reply.getClass().getSimpleName());
    }

    private SagaReply reserve(ReserveInventory cmd) {
        Optional<Reservation> existing = reservationRepository.findByOrderIdForUpdate(cmd.orderId());
        if (existing.isPresent()) {
            return existing.get().getStatus() == ReservationStatus.RESERVED
                    ? new InventoryReserved(cmd.sagaId(), cmd.orderId())
                    : new InventoryFailed(cmd.sagaId(), cmd.orderId(), "Order already released");
        }
        Optional<Product> product = productRepository.findForUpdate(cmd.productId());
        if (product.isEmpty()) {
            return new InventoryFailed(cmd.sagaId(), cmd.orderId(), "Unknown product " + cmd.productId());
        }
        if (!product.get().hasStock(cmd.quantity())) {
            return new InventoryFailed(cmd.sagaId(), cmd.orderId(), "Insufficient stock");
        }
        product.get().reserve(cmd.quantity());
        reservationRepository.save(new Reservation(cmd.orderId(), cmd.productId(), cmd.quantity()));
        return new InventoryReserved(cmd.sagaId(), cmd.orderId());
    }

    /**
     * Always replies InventoryReleased so the saga can move on to refunding. With no reservation on record it
     * leaves a RELEASED tombstone so a ReserveInventory arriving later (e.g. replayed from the DLT) is refused.
     * Unknown products need no tombstone: reserving them always fails.
     */
    private SagaReply release(ReleaseInventory cmd) {
        Optional<Reservation> existing = reservationRepository.findByOrderIdForUpdate(cmd.orderId());
        if (existing.isEmpty()) {
            if (productRepository.existsById(cmd.productId())) {
                reservationRepository.save(Reservation.tombstone(cmd.orderId(), cmd.productId(), cmd.quantity()));
            }
        } else if (existing.get().getStatus() == ReservationStatus.RESERVED) {
            Reservation reservation = existing.get();
            Product product = productRepository.findForUpdate(reservation.getProductId())
                    .orElseThrow(() -> new IllegalStateException("No product row for " + reservation.getProductId()));
            product.release(reservation.getQuantity());
            reservation.release();
        }
        return new InventoryReleased(cmd.sagaId(), cmd.orderId());
    }
}
