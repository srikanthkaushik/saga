package com.saga.payment.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.saga.payment.domain.Payment;
import com.saga.payment.domain.PaymentRepository;
import com.saga.payment.domain.PaymentStatus;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentRepository paymentRepository;

    public PaymentController(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    /** The payment for an order: COMPLETED, REFUNDED, or a CANCELLED tombstone (refund arrived before any charge). */
    @GetMapping("/{orderId}")
    @Transactional(readOnly = true)
    public ResponseEntity<PaymentView> get(@PathVariable UUID orderId) {
        return ResponseEntity.of(paymentRepository.findByOrderId(orderId).map(PaymentView::of));
    }

    public record PaymentView(UUID id, UUID orderId, String customerId, BigDecimal amount, PaymentStatus status,
                              Instant createdAt, Instant updatedAt) {
        static PaymentView of(Payment p) {
            return new PaymentView(p.getId(), p.getOrderId(), p.getCustomerId(), p.getAmount(), p.getStatus(),
                    p.getCreatedAt(), p.getUpdatedAt());
        }
    }
}
