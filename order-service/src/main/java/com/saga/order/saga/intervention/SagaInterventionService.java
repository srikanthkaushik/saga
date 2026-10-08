package com.saga.order.saga.intervention;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.saga.order.domain.Order;
import com.saga.order.saga.OrderSaga;
import com.saga.order.saga.OrderSagaOrchestrator;
import com.saga.order.saga.OrderSagaRepository;
import com.saga.order.saga.SagaState;
import com.saga.order.saga.SagaTimeoutProperties;

/**
 * Operator actions on compensating sagas. Each runs in one transaction with the saga's optimistic lock, so it
 * cannot interleave with a reply or a timeout on the same saga; the loser of such a race gets an
 * optimistic-lock failure (surfaced as 409 by the endpoint). Every action is recorded in saga_intervention.
 */
@Service
public class SagaInterventionService {

    private static final Logger log = LoggerFactory.getLogger(SagaInterventionService.class);

    private final OrderSagaRepository sagaRepository;
    private final SagaInterventionRepository interventionRepository;
    private final OrderSagaOrchestrator orchestrator;
    private final SagaTimeoutProperties timeouts;

    public SagaInterventionService(OrderSagaRepository sagaRepository,
                                   SagaInterventionRepository interventionRepository,
                                   OrderSagaOrchestrator orchestrator,
                                   SagaTimeoutProperties timeouts) {
        this.sagaRepository = sagaRepository;
        this.interventionRepository = interventionRepository;
        this.orchestrator = orchestrator;
        this.timeouts = timeouts;
    }

    @Transactional(readOnly = true)
    public Optional<SagaDetail> detail(UUID sagaId) {
        return sagaRepository.findById(sagaId).map(saga -> {
            Order order = orchestrator.orderOf(saga);
            return new SagaDetail(saga.getId(), saga.getOrderId(), saga.getState(), saga.getFailureReason(),
                    saga.getCompensationResends(), saga.getCompensatingSince(), saga.getDeadline(), order.getStatus(),
                    interventionRepository.findBySagaIdOrderByCreatedAt(sagaId).stream().map(SagaDetail.Entry::of).toList());
        });
    }

    /**
     * @throws SagaNotFoundException      no such saga
     * @throws SagaNotCompensatingException the saga is not in RELEASING_INVENTORY or COMPENSATING
     */
    @Transactional
    public InterventionResult intervene(UUID sagaId, InterventionAction action, String operator, String note) {
        OrderSaga saga = sagaRepository.findById(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
        if (!saga.getState().isCompensating()) {
            throw new SagaNotCompensatingException(saga);
        }
        Order order = orchestrator.orderOf(saga);
        SagaState from = saga.getState();

        switch (action) {
            case RETRY -> {
                saga.retryCompensationManually(Instant.now().plus(timeouts.compensation()));
                orchestrator.sendCompensation(saga, order);
            }
            case RESOLVE -> {
                saga.resolveManually();
                order.reject(saga.getFailureReason() + " (compensation resolved manually)");
            }
        }

        interventionRepository.save(new SagaIntervention(saga.getId(), action, operator, note, from, saga.getState()));
        log.warn("Operator {} applied {} to saga {} for order {}: {} -> {} ({})",
                operator, action, saga.getId(), order.getId(), from, saga.getState(), note);
        return new InterventionResult(saga.getId(), order.getId(), action, operator, from, saga.getState(),
                saga.getCompensationResends(), saga.getDeadline(), order.getStatus());
    }

    public static class SagaNotFoundException extends RuntimeException {
        SagaNotFoundException(UUID sagaId) {
            super("Unknown saga " + sagaId);
        }
    }

    public static class SagaNotCompensatingException extends RuntimeException {
        SagaNotCompensatingException(OrderSaga saga) {
            super("Saga " + saga.getId() + " is " + saga.getState() + "; only RELEASING_INVENTORY or COMPENSATING sagas can be retried or resolved");
        }
    }
}
