package com.saga.order.saga;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.SecurityContext;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Component;

import com.saga.order.saga.intervention.InterventionAction;
import com.saga.order.saga.intervention.SagaInterventionService;
import com.saga.order.saga.intervention.SagaInterventionService.SagaNotCompensatingException;
import com.saga.order.saga.intervention.SagaInterventionService.SagaNotFoundException;

/**
 * <pre>
 * GET  /actuator/stucksagas            sagas behind a saga.compensation.stuck alert, oldest first
 * GET  /actuator/stucksagas/{sagaId}   one saga with its intervention history
 * POST /actuator/stucksagas/{sagaId}   {"action": "retry"|"resolve", "operator": "...", "note": "..."}
 * </pre>
 * Typical causes of a stuck saga: the participant is down, or its compensation keeps failing and landing on its
 * DLT (check {@code GET /actuator/dlt} on payment/inventory service). Fix the cause, then {@code retry}. Only if
 * the compensation was completed by hand in the participant's records, {@code resolve} (note required).
 * <p>
 * Status codes: 200 applied, 400 bad request, 404 unknown saga, 409 saga not compensating or changed concurrently.
 * Once Spring Security protects actuator, the authenticated principal is recorded as operator.
 */
@Component
@Endpoint(id = "stucksagas")
public class StuckSagasEndpoint {

    private final OrderSagaRepository sagaRepository;
    private final SagaAlertProperties alerts;
    private final SagaInterventionService interventions;

    public StuckSagasEndpoint(OrderSagaRepository sagaRepository,
                              SagaAlertProperties alerts,
                              SagaInterventionService interventions) {
        this.sagaRepository = sagaRepository;
        this.alerts = alerts;
        this.interventions = interventions;
    }

    @ReadOperation
    public List<StuckSaga> stuckSagas() {
        return sagaRepository.findStuck(alerts.stuckAfterResends());
    }

    @ReadOperation
    public WebEndpointResponse<Object> saga(@Selector String sagaId) {
        UUID id = parse(sagaId);
        if (id == null) {
            return error(400, "Invalid saga id '" + sagaId + "'");
        }
        return interventions.detail(id)
                .map(detail -> new WebEndpointResponse<Object>(detail, 200))
                .orElseGet(() -> error(404, "Unknown saga " + sagaId));
    }

    @WriteOperation
    public WebEndpointResponse<Object> intervene(@Selector String sagaId,
                                                 String action,
                                                 @Nullable String operator,
                                                 @Nullable String note,
                                                 SecurityContext securityContext) {
        UUID id = parse(sagaId);
        if (id == null) {
            return error(400, "Invalid saga id '" + sagaId + "'");
        }
        InterventionAction parsedAction = InterventionAction.parse(action).orElse(null);
        if (parsedAction == null) {
            return error(400, "Unknown action '" + action + "'; expected retry or resolve");
        }
        String who = securityContext.getPrincipal() != null ? securityContext.getPrincipal().getName() : operator;
        if (who == null || who.isBlank()) {
            return error(400, "operator is required");
        }
        if (parsedAction == InterventionAction.RESOLVE && (note == null || note.isBlank())) {
            return error(400, "note is required for resolve: say how and where the compensation was completed");
        }
        try {
            return new WebEndpointResponse<>(interventions.intervene(id, parsedAction, who.strip(), note), 200);
        } catch (SagaNotFoundException e) {
            return error(404, e.getMessage());
        } catch (SagaNotCompensatingException e) {
            return error(409, e.getMessage());
        } catch (ObjectOptimisticLockingFailureException e) {
            return error(409, "Saga " + sagaId + " changed concurrently; re-check its state and retry");
        }
    }

    private static UUID parse(String sagaId) {
        try {
            return UUID.fromString(sagaId);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static WebEndpointResponse<Object> error(int status, String message) {
        return new WebEndpointResponse<>(Map.of("error", message), status);
    }
}
