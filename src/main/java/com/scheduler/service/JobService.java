package com.scheduler.service;

import com.scheduler.domain.entity.DeadLetterJob;
import com.scheduler.domain.entity.Job;
import com.scheduler.domain.entity.JobDependency;
import com.scheduler.domain.enums.JobStatus;
import com.scheduler.domain.enums.ScheduleType;
import com.scheduler.dto.CreateJobRequest;
import com.scheduler.dto.ExecutionResponse;
import com.scheduler.dto.JobResponse;
import com.scheduler.exception.InvalidJobRequestException;
import com.scheduler.exception.JobNotFoundException;
import com.scheduler.handler.JobHandlerRegistry;
import com.scheduler.repository.DeadLetterJobRepository;
import com.scheduler.repository.JobDependencyRepository;
import com.scheduler.repository.JobExecutionRepository;
import com.scheduler.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobService {

    private final JobRepository jobRepository;
    private final JobExecutionRepository executionRepository;
    private final DeadLetterJobRepository dlqRepository;
    private final JobDependencyRepository dependencyRepository;
    private final JobHandlerRegistry handlerRegistry;
    private final DagEvaluatorService dagEvaluator;

    @Transactional
    public JobResponse registerJob(CreateJobRequest request) {
        if (!handlerRegistry.isRegistered(request.getJobType())) {
            throw new InvalidJobRequestException(
                "No handler registered for jobType: '" + request.getJobType() +
                "'. Available types: " + handlerRegistry.registeredTypes());
        }

        // Phase 3: validate dependencies exist and are ONE_TIME jobs
        boolean hasDependencies = request.getDependsOn() != null && !request.getDependsOn().isEmpty();
        if (hasDependencies) {
            validateDependencies(request.getDependsOn());
        }

        Instant firstRunAt = computeFirstRunAt(request);

        // Jobs with unmet dependencies start WAITING — invisible to the scheduler poll.
        // Jobs with no dependencies start SCHEDULED immediately.
        JobStatus initialStatus = hasDependencies ? JobStatus.WAITING : JobStatus.SCHEDULED;

        Job job = Job.builder()
                .name(request.getName())
                .jobType(request.getJobType())
                .scheduleType(request.getScheduleType())
                .cronExpr(request.getCronExpr())
                .payload(request.getPayload())
                .maxRetries(request.getMaxRetries())
                .status(initialStatus)
                .nextRunAt(firstRunAt)
                .build();

        Job saved = jobRepository.save(job);

        // Phase 3: persist dependency edges after the job exists in DB
        if (hasDependencies) {
            // Cycle check before committing edges
            dagEvaluator.validateNoCycles(saved.getId(), request.getDependsOn());

            for (UUID depId : request.getDependsOn()) {
                Job dependency = jobRepository.findById(depId)
                        .orElseThrow(() -> new JobNotFoundException(depId));

                dependencyRepository.save(JobDependency.builder()
                        .job(saved)
                        .dependsOnJob(dependency)
                        .build());
            }
            log.info("Job registered WAITING: id={}, dependencies={}", saved.getId(), request.getDependsOn());
        } else {
            log.info("Job registered SCHEDULED: id={}, type={}, firstRunAt={}",
                    saved.getId(), saved.getJobType(), firstRunAt);
        }

        return JobResponse.from(saved);
    }

    private void validateDependencies(List<UUID> dependsOn) {
        for (UUID depId : dependsOn) {
            Job dep = jobRepository.findById(depId)
                    .orElseThrow(() -> new InvalidJobRequestException(
                            "Dependency job not found: " + depId));
            if (dep.getScheduleType() == ScheduleType.CRON) {
                throw new InvalidJobRequestException(
                        "CRON jobs cannot be used as dependencies (they never reach COMPLETED). " +
                        "Offending dependency id: " + depId);
            }
        }
    }

    private Instant computeFirstRunAt(CreateJobRequest request) {
        if (request.getScheduleType() == ScheduleType.CRON) {
            if (request.getCronExpr() == null || request.getCronExpr().isBlank()) {
                throw new InvalidJobRequestException("cronExpr is required for CRON schedule type");
            }
            try {
                CronExpression cron = CronExpression.parse(request.getCronExpr());
                LocalDateTime next = cron.next(LocalDateTime.now(ZoneOffset.UTC));
                if (next == null) throw new InvalidJobRequestException(
                        "Cron expression produces no future execution times");
                return next.toInstant(ZoneOffset.UTC);
            } catch (IllegalArgumentException e) {
                throw new InvalidJobRequestException("Invalid cron expression: " + e.getMessage());
            }
        }
        if (request.getScheduleType() == ScheduleType.ONE_TIME) {
            if (request.getRunAt() == null)
                throw new InvalidJobRequestException("runAt is required for ONE_TIME schedule type");
            if (request.getRunAt().isBefore(Instant.now()))
                throw new InvalidJobRequestException("runAt must be in the future");
            return request.getRunAt();
        }
        throw new InvalidJobRequestException("Unsupported schedule type: " + request.getScheduleType());
    }

    @Transactional(readOnly = true)
    public JobResponse getJob(UUID id) {
        return JobResponse.from(findOrThrow(id));
    }

    @Transactional(readOnly = true)
    public List<JobResponse> getAllJobs() {
        return jobRepository.findAll().stream().map(JobResponse::from).collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public List<ExecutionResponse> getExecutionHistory(UUID jobId) {
        findOrThrow(jobId);
        return executionRepository.findByJobIdOrderByStartedAtDesc(jobId)
                .stream().map(ExecutionResponse::from).collect(Collectors.toList());
    }

    @Transactional
    public JobResponse pauseJob(UUID id) {
        Job job = findOrThrow(id);
        if (job.getStatus() != JobStatus.SCHEDULED)
            throw new InvalidJobRequestException(
                    "Only SCHEDULED jobs can be paused. Current status: " + job.getStatus());
        jobRepository.updateStatus(id, JobStatus.PAUSED, Instant.now());
        return JobResponse.from(findOrThrow(id));
    }

    @Transactional
    public JobResponse resumeJob(UUID id) {
        Job job = findOrThrow(id);
        if (job.getStatus() != JobStatus.PAUSED)
            throw new InvalidJobRequestException(
                    "Only PAUSED jobs can be resumed. Current status: " + job.getStatus());
        jobRepository.updateStatus(id, JobStatus.SCHEDULED, Instant.now());
        return JobResponse.from(findOrThrow(id));
    }

    @Transactional
    public JobResponse triggerNow(UUID id) {
        Job job = findOrThrow(id);
        if (job.getStatus() == JobStatus.RUNNING)
            throw new InvalidJobRequestException("Job is already running");
        if (job.getStatus() == JobStatus.DEAD)
            throw new InvalidJobRequestException("Dead jobs cannot be triggered. Requeue from DLQ first.");
        if (job.getStatus() == JobStatus.WAITING)
            throw new InvalidJobRequestException("Job has unmet dependencies. Cannot trigger manually.");
        job.setNextRunAt(Instant.now());
        job.setStatus(JobStatus.SCHEDULED);
        return JobResponse.from(jobRepository.save(job));
    }

    @Transactional
    public void deleteJob(UUID id) {
        findOrThrow(id);
        jobRepository.deleteById(id);
    }

    @Transactional(readOnly = true)
    public List<DeadLetterJob> getDlqJobs() {
        return dlqRepository.findAllByOrderByFailedAtDesc();
    }

    @Transactional
    public JobResponse requeueFromDlq(UUID dlqId) {
        DeadLetterJob dlqEntry = dlqRepository.findById(dlqId)
                .orElseThrow(() -> new JobNotFoundException(dlqId));
        Job job = dlqEntry.getJob();
        job.setStatus(JobStatus.SCHEDULED);
        job.setAttemptCount(0);
        job.setNextRunAt(Instant.now());
        jobRepository.save(job);
        dlqRepository.delete(dlqEntry);
        log.info("Job requeued from DLQ: jobId={}", job.getId());
        return JobResponse.from(job);
    }

    private Job findOrThrow(UUID id) {
        return jobRepository.findById(id).orElseThrow(() -> new JobNotFoundException(id));
    }
}
