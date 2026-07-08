package com.scheduler.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component
public class JobHandlerRegistry {

    private final Map<String, JobHandler> registry = new HashMap<>();

    public void register(String jobType, JobHandler handler) {
        if (registry.containsKey(jobType)) {
            throw new IllegalStateException(
                "Handler already registered for jobType: " + jobType);
        }
        registry.put(jobType, handler);
        log.info("Registered handler for jobType: {}", jobType);
    }

    /**
     * Returns the handler for a given jobType.
     * Throws if the type is unknown — we want to fail loudly rather than silently
     * swallow a dispatch for a job type that no longer has a handler.
     */
    public JobHandler getHandler(String jobType) {
        JobHandler handler = registry.get(jobType);
        if (handler == null) {
            throw new IllegalArgumentException(
                "No handler registered for jobType: '" + jobType +
                "'. Registered types: " + registry.keySet());
        }
        return handler;
    }

    public boolean isRegistered(String jobType) {
        return registry.containsKey(jobType);
    }

    public Set<String> registeredTypes() {
        return registry.keySet();
    }
}
