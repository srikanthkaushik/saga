package com.saga.common.idempotency;

import java.util.UUID;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Must run in the same transaction as the handler's side effects: if the handler rolls back, the
 * processed marker rolls back with it and the redelivered message is handled again.
 */
@Component
public class IdempotencyGuard {

    private final ProcessedMessageRepository repository;

    public IdempotencyGuard(ProcessedMessageRepository repository) {
        this.repository = repository;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public boolean firstDelivery(UUID messageId) {
        return repository.markProcessed(messageId) == 1;
    }
}
