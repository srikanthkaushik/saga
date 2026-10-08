package com.saga.common.outbox;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.saga.common.messaging.MessageCodec;

/**
 * Records an outgoing message in the caller's transaction. MANDATORY propagation makes it impossible
 * to publish outside the business transaction, which is the whole point of the outbox.
 */
@Component
public class OutboxWriter {

    private final OutboxRepository outboxRepository;
    private final MessageCodec codec;

    public OutboxWriter(OutboxRepository outboxRepository, MessageCodec codec) {
        this.outboxRepository = outboxRepository;
        this.codec = codec;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void write(String topic, String key, Object message) {
        outboxRepository.save(new OutboxMessage(topic, key, codec.typeName(message), codec.serialize(message)));
    }
}
