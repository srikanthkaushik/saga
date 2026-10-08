package com.saga.order.saga;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Periodically times out sagas past their deadline. Each saga is handled in its own transaction so one
 * failure doesn't hold back the rest; a saga concurrently advanced by a reply loses the optimistic lock
 * here and is simply re-evaluated on the next scan. Safe to run on several instances.
 */
@Component
public class SagaTimeoutScanner {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutScanner.class);

    private final OrderSagaRepository sagaRepository;
    private final OrderSagaOrchestrator orchestrator;
    private final TransactionTemplate transactionTemplate;
    private final SagaTimeoutProperties timeouts;

    public SagaTimeoutScanner(OrderSagaRepository sagaRepository,
                              OrderSagaOrchestrator orchestrator,
                              TransactionTemplate transactionTemplate,
                              SagaTimeoutProperties timeouts) {
        this.sagaRepository = sagaRepository;
        this.orchestrator = orchestrator;
        this.transactionTemplate = transactionTemplate;
        this.timeouts = timeouts;
    }

    @Scheduled(fixedDelayString = "${saga.timeout.scan-interval:5s}")
    public void timeOutOverdueSagas() {
        Instant now = Instant.now();
        List<UUID> overdue = sagaRepository.findOverdueIds(now, timeouts.batchSize());
        for (UUID sagaId : overdue) {
            try {
                transactionTemplate.executeWithoutResult(tx -> orchestrator.onTimeout(sagaId, now));
            } catch (ObjectOptimisticLockingFailureException e) {
                log.debug("Saga {} changed concurrently, re-evaluating next scan", sagaId);
            } catch (RuntimeException e) {
                log.error("Timing out saga {} failed", sagaId, e);
            }
        }
    }
}
