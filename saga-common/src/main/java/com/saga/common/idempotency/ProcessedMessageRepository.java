package com.saga.common.idempotency;

import java.time.Instant;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, UUID> {

    /**
     * @return 1 if this is the first time the message is seen, 0 if it is a duplicate
     */
    @Modifying
    @Query(value = """
            insert into processed_message (message_id, processed_at)
            values (:messageId, now())
            on conflict (message_id) do nothing
            """, nativeQuery = true)
    int markProcessed(@Param("messageId") UUID messageId);

    /** Deletes up to {@code limit} markers recorded before {@code cutoff}. */
    @Modifying
    @Query(value = """
            delete from processed_message
            where message_id in (
                select message_id from processed_message
                where processed_at < :cutoff
                limit :limit
                for update skip locked)
            """, nativeQuery = true)
    int deleteProcessedBefore(@Param("cutoff") Instant cutoff, @Param("limit") int limit);
}
