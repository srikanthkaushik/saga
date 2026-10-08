package com.saga.order.saga;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OrderSagaRepository extends JpaRepository<OrderSaga, UUID> {

    Optional<OrderSaga> findByOrderId(UUID orderId);

    @Query(value = """
            select id from order_saga
            where deadline <= :now
            order by deadline
            limit :limit
            """, nativeQuery = true)
    List<UUID> findOverdueIds(@Param("now") Instant now, @Param("limit") int limit);

    @Query("""
            select new com.saga.order.saga.StuckSaga(
                s.id, s.orderId, s.state, s.failureReason,
                s.compensationResends, s.compensatingSince, s.deadline)
            from OrderSaga s
            where s.state in (com.saga.order.saga.SagaState.RELEASING_INVENTORY, com.saga.order.saga.SagaState.COMPENSATING)
              and s.compensationResends >= :threshold
            order by s.compensatingSince
            """)
    List<StuckSaga> findStuck(@Param("threshold") int threshold);

    @Query(value = """
            select count(*)                                                              as compensating,
                   count(*) filter (where compensation_resends >= :threshold)            as stuck,
                   coalesce(extract(epoch from now() - min(compensating_since)), 0)::float8 as oldest_age_seconds
            from order_saga
            where state in ('RELEASING_INVENTORY', 'COMPENSATING')
            """, nativeQuery = true)
    CompensationStats compensationStats(@Param("threshold") int threshold);

    interface CompensationStats {

        long getCompensating();

        long getStuck();

        double getOldestAgeSeconds();
    }
}
