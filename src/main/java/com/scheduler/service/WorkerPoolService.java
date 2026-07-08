package com.scheduler.service;

import com.scheduler.config.NodeIdentityService;
import com.scheduler.domain.entity.DeadLetterJob;
import com.scheduler.domain.entity.Job;
import com.scheduler.domain.entity.JobExecution;
import com.scheduler.domain.enums.ExecutionStatus;
import com.scheduler.domain.enums.JobStatus;
import com.scheduler.domain.enums.ScheduleType;
import com.scheduler.handler.JobHandlerRegistry;
import com.scheduler.repository.DeadLetterJobRepository;
import com.scheduler.repository.JobExecutionRepository;
import com.scheduler.repository.JobRepository;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class WorkerPoolService {

    private final JobRepository jobRepository;
    private final JobExecutionRepository executionRepository;
    private final DeadLetterJobRepository dlqRepository;
    private final JobHandlerRegistry handlerRegistry;
    private final NodeIdentityService nodeIdentity;
    private final DistributedLockService distributedLock;
    private final DagEvaluatorService dagEvaluator;
    private final SchedulerMetricsService metrics;          // Phase 4

    @Value("${lock.job-ttl-minutes:5}")
    private long jobLockTtlMinutes;

    @Async("jobExecutorPool")
    public CompletableFuture<Void> executeJob(Job job) {
        String lockKey   = "job:lock:" + job.getId();
        String lockValue = DistributedLockService.generateLockValue(nodeIdentity.getNodeId());
        Duration lockTtl = Duration.ofMinutes(jobLockTtlMinutes);

        boolean locked = distributedLock.acquire(lockKey, lockValue, lockTtl);
        if (!locked) {
            log.warn("[WORKER] Lock unavailable for job id={}. Skipping.", job.getId());
            return CompletableFuture.completedFuture(null);
        }

        String workerId = nodeIdentity.getNodeId() + "-" + Thread.currentThread().getName();
        log.info("[WORKER] Executing job: id={}, type={}, attempt={}, worker={}",
                job.getId(), job.getJobType(), job.getAttemptCount() + 1, workerId);

        JobExecution execution = createExecutionRecord(job, workerId);
        // Phase 4: start wall-clock timer before handler runs
        Timer.Sample timerSample = metrics.startExecutionTimer();
        boolean success = false;

        try {
            handlerRegistry.getHandler(job.getJobType()).execute(job.getPayload());
            success = true;
            markSucceeded(job, execution);
            metrics.recordSuccess(job.getJobType());

        } catch (Exception ex) {
            log.error("[WORKER] Job failed: id={}, attempt={}, error={}",
                    job.getId(), execution.getAttemptNumber(), ex.getMessage());
            markFailed(job, execution, ex);
            metrics.recordFailure(job.getJobType());

        } finally {
            metrics.stopExecutionTimer(timerSample, job.getJobType(), success);
            distributedLock.release(lockKey, lockValue);
        }

        return CompletableFuture.completedFuture(null);
    }

    @Transactional
    protected JobExecution createExecutionRecord(Job job, String workerId) {
        JobExecution execution = JobExecution.builder()
                .job(job)
                .attemptNumber(job.getAttemptCount() + 1)
                .status(ExecutionStatus.RUNNING)
                .workerId(workerId)
                .startedAt(Instant.now())
                .build();
        return executionRepository.save(execution);
    }

    @Transactional
    protected void markSucceeded(Job job, JobExecution execution) {
        execution.setStatus(ExecutionStatus.SUCCEEDED);
        execution.setFinishedAt(Instant.now());
        executionRepository.save(execution);

        if (job.getScheduleType() == ScheduleType.ONE_TIME) {
            jobRepository.updateStatus(job.getId(), JobStatus.COMPLETED, Instant.now());
            log.info("[WORKER] ONE_TIME job completed: id={}", job.getId());
            dagEvaluator.evaluateDependents(job.getId());
        } else {
            Instant nextRun = computeNextCronRun(job.getCronExpr());
            jobRepository.reschedule(job.getId(), nextRun, Instant.now());
            log.info("[WORKER] CRON job rescheduled: id={}, nextRunAt={}", job.getId(), nextRun);
        }
    }

    @Transactional
    protected void markFailed(Job job, JobExecution execution, Exception ex) {
        execution.setStatus(ExecutionStatus.FAILED);
        execution.setFinishedAt(Instant.now());
        execution.setErrorMessage(truncate(ex.getMessage(), 1000));
        executionRepository.save(execution);

        int nextAttempt = job.getAttemptCount() + 1;

        if (nextAttempt >= job.getMaxRetries()) {
            jobRepository.updateStatus(job.getId(), JobStatus.DEAD, Instant.now());
            DeadLetterJob dlqEntry = DeadLetterJob.builder()
                    .job(job)
                    .lastExecution(execution)
                    .failureReason(ex.getMessage())
                    .build();
            dlqRepository.save(dlqEntry);
            metrics.recordDlqAddition(job.getJobType());   // Phase 4
            log.warn("[WORKER] Job moved to DLQ after {} attempts: id={}", nextAttempt, job.getId());
        } else {
            long backoffSeconds = Math.min((long) Math.pow(2, nextAttempt), 3600);
            Instant retryAt = Instant.now().plusSeconds(backoffSeconds);
            jobRepository.scheduleRetry(job.getId(), nextAttempt, retryAt, Instant.now());
            log.info("[WORKER] Job retry scheduled: id={}, attempt={}/{}, retryAt={}",
                    job.getId(), nextAttempt, job.getMaxRetries(), retryAt);
        }
    }

    private Instant computeNextCronRun(String cronExpr) {
        CronExpression cron = CronExpression.parse(cronExpr);
        LocalDateTime next = cron.next(LocalDateTime.now(ZoneOffset.UTC));
        if (next == null) throw new IllegalStateException(
                "Cron expression produced no next execution: " + cronExpr);
        return next.toInstant(ZoneOffset.UTC);
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
