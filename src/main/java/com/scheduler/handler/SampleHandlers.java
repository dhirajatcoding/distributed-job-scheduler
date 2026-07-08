package com.scheduler.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

// ─── Email Notification Handler ───────────────────────────────────────────────

@Slf4j
@Component
class EmailNotificationHandler implements JobHandler {

    @Override
    public void execute(String payload) throws Exception {
        log.info("[EMAIL] Executing with payload: {}", payload);
        // Simulate SMTP call latency
        Thread.sleep(500);
        log.info("[EMAIL] Notification sent successfully");
    }
}

// ─── Report Generation Handler ────────────────────────────────────────────────
// Always fails — use this job type to test retry + DLQ behavior

@Slf4j
@Component
class ReportGenerationHandler implements JobHandler {

    @Override
    public void execute(String payload) throws Exception {
        log.info("[REPORT] Attempting report generation with payload: {}", payload);
        Thread.sleep(200);
        throw new RuntimeException("Simulated report generation failure — testing retry flow");
    }
}

// ─── Handler Registrar ────────────────────────────────────────────────────────
// Registers all handlers with their string keys at application startup.
// Adding a new job type = implement JobHandler + add one line here.

@Slf4j
@Configuration
@RequiredArgsConstructor
class HandlerRegistrar {

    private final JobHandlerRegistry registry;
    private final EmailNotificationHandler emailHandler;
    private final ReportGenerationHandler reportHandler;

    @PostConstruct
    public void registerHandlers() {
        registry.register("EMAIL_NOTIFICATION", emailHandler);
        registry.register("REPORT_GENERATION", reportHandler);
        log.info("All job handlers registered. Types: {}", registry.registeredTypes());
    }
}
