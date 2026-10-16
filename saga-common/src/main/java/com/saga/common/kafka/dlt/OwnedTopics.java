package com.saga.common.kafka.dlt;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

/**
 * The source topics this service's listeners consume. A service owns exactly the DLTs of these topics: it can
 * report and replay them, and nothing else.
 */
@Component
public class OwnedTopics {

    private final KafkaListenerEndpointRegistry listenerRegistry;

    public OwnedTopics(KafkaListenerEndpointRegistry listenerRegistry) {
        this.listenerRegistry = listenerRegistry;
    }

    public Set<String> get() {
        Set<String> topics = new TreeSet<>();
        listenerRegistry.getListenerContainers().forEach(container -> {
            String[] containerTopics = container.getContainerProperties().getTopics();
            if (containerTopics != null) {
                topics.addAll(Arrays.asList(containerTopics));
            }
        });
        return topics;
    }
}
