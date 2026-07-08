package com.scheduler.domain.enums;

public enum ScheduleType {
    CRON,       // Repeating schedule defined by a cron expression
    ONE_TIME    // Runs once at the specified next_run_at time
}
