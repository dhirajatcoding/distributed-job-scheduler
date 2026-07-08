package com.scheduler.exception;

public class InvalidJobRequestException extends RuntimeException {
    public InvalidJobRequestException(String message) {
        super(message);
    }
}
