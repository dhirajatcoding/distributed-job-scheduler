package com.scheduler.dto;

import com.scheduler.domain.enums.ScheduleType;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Data
public class CreateJobRequest {

    @NotBlank(message = "Job name is required")
    private String name;

    // Must match a key registered in JobHandlerRegistry
    @NotBlank(message = "Job type is required")
    private String jobType;

    @NotNull(message = "Schedule type is required")
    private ScheduleType scheduleType;

    // Required for CRON jobs, ignored for ONE_TIME
    private String cronExpr;

    // Required for ONE_TIME jobs. For CRON jobs, first run time is computed from cronExpr.
    private Instant runAt;

    // Arbitrary JSON — passed as-is to the handler
    private String payload;

    @Min(value = 0, message = "maxRetries cannot be negative")
    private int maxRetries = 3;

    // Phase 3: optional list of job IDs that must reach COMPLETED before this job runs.
    // Only valid for ONE_TIME jobs as dependencies (CRON jobs never reach COMPLETED).
    // If non-empty, the registered job starts in WAITING status, not SCHEDULED.
    private List<UUID> dependsOn = new java.util.ArrayList<>();
}
