package com.saga.common.outbox;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {

    @Query(value = """
            select * from outbox
            where published_at is null
            order by created_at
            limit :limit
            for update skip locked
            """, nativeQuery = true)
    List<OutboxMessage> lockUnpublishedBatch(@Param("limit") int limit);

    List<OutboxMessage> findByMessageKeyOrderByCreatedAt(String messageKey);
}
