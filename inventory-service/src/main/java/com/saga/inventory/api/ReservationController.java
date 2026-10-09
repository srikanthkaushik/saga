package com.saga.inventory.api;

import java.time.Instant;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.saga.inventory.domain.Reservation;
import com.saga.inventory.domain.ReservationRepository;
import com.saga.inventory.domain.ReservationStatus;

@RestController
@RequestMapping("/reservations")
public class ReservationController {

    private final ReservationRepository reservationRepository;

    public ReservationController(ReservationRepository reservationRepository) {
        this.reservationRepository = reservationRepository;
    }

    /** The reservation for an order: RESERVED, or RELEASED (stock returned, or a tombstone written before any reserve). */
    @GetMapping("/{orderId}")
    @Transactional(readOnly = true)
    public ResponseEntity<ReservationView> get(@PathVariable UUID orderId) {
        return ResponseEntity.of(reservationRepository.findByOrderId(orderId).map(ReservationView::of));
    }

    public record ReservationView(UUID id, UUID orderId, String productId, int quantity, ReservationStatus status,
                                  Instant createdAt, Instant releasedAt) {
        static ReservationView of(Reservation r) {
            return new ReservationView(r.getId(), r.getOrderId(), r.getProductId(), r.getQuantity(), r.getStatus(),
                    r.getCreatedAt(), r.getReleasedAt());
        }
    }
}
