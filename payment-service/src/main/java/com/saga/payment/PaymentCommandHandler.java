package com.saga.payment;

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
import com.saga.common.messaging.SagaCommand.ProcessPayment;
import com.saga.common.messaging.SagaCommand.RefundPayment;
import com.saga.common.messaging.SagaReply;
import com.saga.common.messaging.SagaReply.PaymentFailed;
import com.saga.common.messaging.SagaReply.PaymentProcessed;
import com.saga.common.messaging.SagaReply.PaymentRefunded;
import com.saga.common.messaging.Topics;
import com.saga.common.outbox.OutboxWriter;
import com.saga.payment.domain.CustomerCredit;
import com.saga.payment.domain.CustomerCreditRepository;
import com.saga.payment.domain.Payment;
import com.saga.payment.domain.PaymentRepository;
import com.saga.payment.domain.PaymentStatus;

@Component
public class PaymentCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandHandler.class);

    private final MessageCodec codec;
    private final IdempotencyGuard idempotencyGuard;
    private final CustomerCreditRepository creditRepository;
    private final PaymentRepository paymentRepository;
    private final OutboxWriter outbox;

    public PaymentCommandHandler(MessageCodec codec,
                                 IdempotencyGuard idempotencyGuard,
                                 CustomerCreditRepository creditRepository,
                                 PaymentRepository paymentRepository,
                                 OutboxWriter outbox) {
        this.codec = codec;
        this.idempotencyGuard = idempotencyGuard;
        this.creditRepository = creditRepository;
        this.paymentRepository = paymentRepository;
        this.outbox = outbox;
    }

    @KafkaListener(topics = Topics.PAYMENT_COMMANDS)
    @Transactional
    public void onCommand(ConsumerRecord<String, String> record) {
        InboundMessage inbound = codec.decode(record);
        if (!idempotencyGuard.firstDelivery(inbound.messageId())) {
            log.debug("Duplicate command {} ignored", inbound.messageId());
            return;
        }

        SagaReply reply = switch (inbound.payload()) {
            case ProcessPayment cmd -> process(cmd);
            case RefundPayment cmd -> refund(cmd);
            default -> throw new NonRetryableMessageException(
                    "Unsupported message on " + record.topic() + ": " + inbound.payload().getClass().getSimpleName());
        };
        outbox.write(Topics.SAGA_REPLIES, reply.orderId().toString(), reply);
        log.info("Order {} -> {}", reply.orderId(), reply.getClass().getSimpleName());
    }

    private SagaReply process(ProcessPayment cmd) {
        Optional<Payment> existing = paymentRepository.findByOrderIdForUpdate(cmd.orderId());
        if (existing.isPresent()) {
            return existing.get().getStatus() == PaymentStatus.COMPLETED
                    ? new PaymentProcessed(cmd.sagaId(), cmd.orderId())
                    : new PaymentFailed(cmd.sagaId(), cmd.orderId(), "Order already " + existing.get().getStatus());
        }
        Optional<CustomerCredit> credit = creditRepository.findForUpdate(cmd.customerId());
        if (credit.isEmpty()) {
            return new PaymentFailed(cmd.sagaId(), cmd.orderId(), "Unknown customer " + cmd.customerId());
        }
        if (!credit.get().canAfford(cmd.amount())) {
            return new PaymentFailed(cmd.sagaId(), cmd.orderId(), "Insufficient credit");
        }
        credit.get().debit(cmd.amount());
        paymentRepository.save(new Payment(cmd.orderId(), cmd.customerId(), cmd.amount()));
        return new PaymentProcessed(cmd.sagaId(), cmd.orderId());
    }

    /**
     * Always replies PaymentRefunded, even when there is nothing to refund, so the saga can finish. With no
     * charge on record it leaves a CANCELLED tombstone so a ProcessPayment arriving later (e.g. replayed from
     * the DLT) is refused instead of charging for a saga that has already given up.
     */
    private SagaReply refund(RefundPayment cmd) {
        Optional<Payment> existing = paymentRepository.findByOrderIdForUpdate(cmd.orderId());
        if (existing.isEmpty()) {
            paymentRepository.save(Payment.cancelled(cmd.orderId()));
        } else if (existing.get().getStatus() == PaymentStatus.COMPLETED) {
            Payment payment = existing.get();
            CustomerCredit credit = creditRepository.findForUpdate(payment.getCustomerId())
                    .orElseThrow(() -> new IllegalStateException("No credit row for " + payment.getCustomerId()));
            credit.credit(payment.getAmount());
            payment.refund();
        }
        return new PaymentRefunded(cmd.sagaId(), cmd.orderId());
    }
}
