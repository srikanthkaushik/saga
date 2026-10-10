package com.saga.shipping;

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
import com.saga.common.messaging.SagaCommand.CancelShipment;
import com.saga.common.messaging.SagaCommand.ScheduleShipment;
import com.saga.common.messaging.SagaReply;
import com.saga.common.messaging.SagaReply.ShipmentCancelled;
import com.saga.common.messaging.SagaReply.ShipmentFailed;
import com.saga.common.messaging.SagaReply.ShipmentScheduled;
import com.saga.common.messaging.Topics;
import com.saga.common.outbox.OutboxWriter;
import com.saga.shipping.domain.Shipment;
import com.saga.shipping.domain.ShipmentRepository;
import com.saga.shipping.domain.ShipmentStatus;
import com.saga.shipping.domain.ShippingZone;
import com.saga.shipping.domain.ShippingZoneRepository;

@Component
public class ShippingCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(ShippingCommandHandler.class);

    private final MessageCodec codec;
    private final IdempotencyGuard idempotencyGuard;
    private final ShipmentRepository shipmentRepository;
    private final ShippingZoneRepository zoneRepository;
    private final OutboxWriter outbox;

    public ShippingCommandHandler(MessageCodec codec,
                                  IdempotencyGuard idempotencyGuard,
                                  ShipmentRepository shipmentRepository,
                                  ShippingZoneRepository zoneRepository,
                                  OutboxWriter outbox) {
        this.codec = codec;
        this.idempotencyGuard = idempotencyGuard;
        this.shipmentRepository = shipmentRepository;
        this.zoneRepository = zoneRepository;
        this.outbox = outbox;
    }

    @KafkaListener(topics = Topics.SHIPPING_COMMANDS)
    @Transactional
    public void onCommand(ConsumerRecord<String, String> record) {
        InboundMessage inbound = codec.decode(record);
        if (!idempotencyGuard.firstDelivery(inbound.messageId())) {
            log.debug("Duplicate command {} ignored", inbound.messageId());
            return;
        }

        SagaReply reply = switch (inbound.payload()) {
            case ScheduleShipment cmd -> schedule(cmd);
            case CancelShipment cmd -> cancel(cmd);
            default -> throw new NonRetryableMessageException(
                    "Unsupported message on " + record.topic() + ": " + inbound.payload().getClass().getSimpleName());
        };
        outbox.write(Topics.SAGA_REPLIES, reply.orderId().toString(), reply);
        log.info("Order {} -> {}", reply.orderId(), reply.getClass().getSimpleName());
    }

    private SagaReply schedule(ScheduleShipment cmd) {
        Optional<Shipment> existing = shipmentRepository.findByOrderIdForUpdate(cmd.orderId());
        if (existing.isPresent()) {
            return existing.get().getStatus() == ShipmentStatus.SCHEDULED
                    ? new ShipmentScheduled(cmd.sagaId(), cmd.orderId(), existing.get().getTrackingNumber())
                    : new ShipmentFailed(cmd.sagaId(), cmd.orderId(), "Order already cancelled");
        }
        boolean served = zoneRepository.findById(cmd.destination()).map(ShippingZone::isServed).orElse(false);
        if (!served) {
            return new ShipmentFailed(cmd.sagaId(), cmd.orderId(), "No carrier serves " + cmd.destination());
        }
        Shipment shipment = shipmentRepository.save(
                Shipment.scheduled(cmd.orderId(), cmd.productId(), cmd.quantity(), cmd.destination()));
        return new ShipmentScheduled(cmd.sagaId(), cmd.orderId(), shipment.getTrackingNumber());
    }

    /**
     * Always replies ShipmentCancelled so the saga can move on to releasing stock. With no shipment on record it
     * leaves a CANCELLED tombstone so a ScheduleShipment arriving later (e.g. replayed from the DLT) is refused.
     */
    private SagaReply cancel(CancelShipment cmd) {
        Optional<Shipment> existing = shipmentRepository.findByOrderIdForUpdate(cmd.orderId());
        if (existing.isEmpty()) {
            shipmentRepository.save(Shipment.tombstone(cmd.orderId()));
        } else if (existing.get().getStatus() == ShipmentStatus.SCHEDULED) {
            existing.get().cancel();
        }
        return new ShipmentCancelled(cmd.sagaId(), cmd.orderId());
    }
}
