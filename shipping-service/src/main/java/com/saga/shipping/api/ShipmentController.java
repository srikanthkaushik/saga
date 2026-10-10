package com.saga.shipping.api;

import java.time.Instant;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.saga.shipping.domain.Shipment;
import com.saga.shipping.domain.ShipmentRepository;
import com.saga.shipping.domain.ShipmentStatus;

@RestController
@RequestMapping("/shipments")
public class ShipmentController {

    private final ShipmentRepository shipmentRepository;

    public ShipmentController(ShipmentRepository shipmentRepository) {
        this.shipmentRepository = shipmentRepository;
    }

    /** The shipment for an order: SCHEDULED, or CANCELLED (cancelled, or a tombstone written before any schedule). */
    @GetMapping("/{orderId}")
    @Transactional(readOnly = true)
    public ResponseEntity<ShipmentView> get(@PathVariable UUID orderId) {
        return ResponseEntity.of(shipmentRepository.findByOrderId(orderId).map(ShipmentView::of));
    }

    public record ShipmentView(UUID id, UUID orderId, String productId, int quantity, String destination,
                               ShipmentStatus status, String trackingNumber, Instant createdAt, Instant cancelledAt) {
        static ShipmentView of(Shipment s) {
            return new ShipmentView(s.getId(), s.getOrderId(), s.getProductId(), s.getQuantity(), s.getDestination(),
                    s.getStatus(), s.getTrackingNumber(), s.getCreatedAt(), s.getCancelledAt());
        }
    }
}
