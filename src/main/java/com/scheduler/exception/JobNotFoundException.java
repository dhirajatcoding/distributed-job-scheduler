package com.scheduler.exception;

import java.util.UUID;

public class JobNotFoundException extends RuntimeException {
    public JobNotFoundException(UUID id) {
        super("No job found with id: " + id);
    }
}
