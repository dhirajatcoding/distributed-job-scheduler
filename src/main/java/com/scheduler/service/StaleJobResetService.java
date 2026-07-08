package com.scheduler.service;

import com.scheduler.domain.entity.Job;
import com.scheduler.domain.enums.JobStatus;
import com.scheduler.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class StaleJobResetService {

    private final JobRepository jobRepository;
    private final SchedulerMetricsService metrics;   // Phase 4

    @Value("${lock.job-ttl-minutes:5}")
    private long lockTtlMinutes;

    /**
     * Runs every minute. Finds RUNNING jobs that have been stuck longer than
     * the lock TTL + 1 minute safety buffer.
     *
     * Why the buffer? The lock TTL is the max time any healthy execution holds
     * the lock. After TTL minutes with no Redis lock, the executing node is
     * definitely dead. The +1 minute gives a little slack for clock drift and
     * slow DB commits before we intervene.
     *
     * What we do on detection:
     *   - Log a warning (visible in dashboards/alerts)
     *   - Reset status to SCHEDULED with next_run_at = NOW()
     *   - Keep attempt_count unchanged — this counts as an attempt
     *   - The scheduler picks it up on the next tick and re-executes
     *
     * What we intentionally do NOT do:
     *   - We don't immediately move it to DLQ. One stale reset is not a failure,
     *     it's a recovery. The retry mechanism handles eventual DLQ promotion.
     *   - We don't release the Redis lock — it's already expired by definition.
     */
    @Scheduled(fixedDelayString = "${scheduler.stale-detection-interval-ms:60000}")
    @Transactional
    public void detectAndResetStaleJobs() {
        // Jobs that have been RUNNING longer than TTL + 1 minute buffer
        Instant staleThreshold = Instant.now()
                .minus(lockTtlMinutes + 1, ChronoUnit.MINUTES);

        List<Job> staleJobs = jobRepository.findStaleRunningJobs(staleThreshold);

        if (staleJobs.isEmpty()) {
            log.debug("[STALE] No stale RUNNING jobs detected");
            return;
        }

        log.warn("[STALE] Detected {} stale RUNNING job(s). Resetting for re-execution.", staleJobs.size());

        for (Job job : staleJobs) {
            log.warn("[STALE] Resetting job: id={}, type={}, runningFor={}min, attempt={}/{}",
                    job.getId(),
                    job.getJobType(),
                    ChronoUnit.MINUTES.between(job.getUpdatedAt(), Instant.now()),
                    job.getAttemptCount(),
                    job.getMaxRetries());

            // Reset to SCHEDULED with immediate next_run_at.
            // Keep attemptCount unchanged — stale reset counts as a used attempt
            // to prevent infinite crash-reset-crash loops on a truly broken job.
            jobRepository.scheduleRetry(
                    job.getId(),
                    job.getAttemptCount(),   // don't increment — just reset status
                    Instant.now(),           // re-run immediately
                    Instant.now()
            );
        }

        log.warn("[STALE] Reset {} stale job(s) to SCHEDULED", staleJobs.size());
        metrics.recordStaleReset(staleJobs.size());   // Phase 4
    }
}
