package com.scheduler.domain.enums;

public enum JobStatus {
    SCHEDULED,  // Ready — waiting for next_run_at to arrive
    RUNNING,    // Claimed by a worker, execution in progress
    PAUSED,     // Manually paused — scheduler skips this job
    COMPLETED,  // ONE_TIME job that finished successfully
    DEAD,       // Exhausted all retries — moved to dead_letter_jobs
    WAITING     // Phase 3: has unmet dependencies — invisible to the scheduler poll
}
