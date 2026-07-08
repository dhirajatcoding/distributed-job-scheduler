package com.scheduler.service;

import com.scheduler.domain.enums.JobStatus;
import com.scheduler.repository.JobRepository;
import io.micrometer.core.instrument.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SchedulerMetricsService {

    private final MeterRegistry registry;

    // Rolled-up counters — total across all job types
    private final Counter totalSuccessCounter;
    private final Counter totalFailureCounter;
    private final Counter dlqCounter;
    private final Counter leaderElectionCounter;
    private final Counter staleResetCounter;

    public SchedulerMetricsService(MeterRegistry registry, JobRepository jobRepository) {
        this.registry = registry;

        // ─── Counters ────────────────────────────────────────────────────────
        this.totalSuccessCounter = Counter.builder("scheduler.jobs.succeeded")
                .description("Total jobs executed successfully across all types")
                .register(registry);

        this.totalFailureCounter = Counter.builder("scheduler.jobs.failed")
                .description("Total failed job execution attempts across all types")
                .register(registry);

        this.dlqCounter = Counter.builder("scheduler.jobs.dlq.added")
                .description("Jobs moved to the dead letter queue after exhausting retries")
                .register(registry);

        this.leaderElectionCounter = Counter.builder("scheduler.leader.elections.won")
                .description("Number of times this node won leader election")
                .register(registry);

        this.staleResetCounter = Counter.builder("scheduler.jobs.stale.reset")
                .description("RUNNING jobs reset to SCHEDULED by the stale detector")
                .register(registry);

        // ─── Gauges — live DB reads ───────────────────────────────────────────
        // Gauges call the lambda on each scrape, not on each job change.
        // This means they're always accurate at read time without any event wiring.
        Gauge.builder("scheduler.queue.depth",
                        jobRepository, repo -> repo.findByStatus(JobStatus.SCHEDULED).size())
                .description("Jobs currently waiting to be dispatched")
                .register(registry);

        Gauge.builder("scheduler.jobs.running",
                        jobRepository, repo -> repo.findByStatus(JobStatus.RUNNING).size())
                .description("Jobs actively executing across all nodes")
                .register(registry);

        Gauge.builder("scheduler.jobs.waiting",
                        jobRepository, repo -> repo.findByStatus(JobStatus.WAITING).size())
                .description("Jobs blocked on unmet DAG dependencies")
                .register(registry);

        Gauge.builder("scheduler.jobs.dead",
                        jobRepository, repo -> repo.findByStatus(JobStatus.DEAD).size())
                .description("Jobs in the dead letter queue")
                .register(registry);
    }

    /**
     * Records a successful execution.
     * Tagged by jobType so you can filter: which handler has the best success rate?
     */
    public void recordSuccess(String jobType) {
        totalSuccessCounter.increment();
        Counter.builder("scheduler.jobs.succeeded")
                .tag("jobType", jobType)
                .register(registry)
                .increment();
    }

    /**
     * Records a failed execution attempt (not necessarily final — may still retry).
     */
    public void recordFailure(String jobType) {
        totalFailureCounter.increment();
        Counter.builder("scheduler.jobs.failed")
                .tag("jobType", jobType)
                .register(registry)
                .increment();
    }

    public void recordDlqAddition(String jobType) {
        dlqCounter.increment();
        Counter.builder("scheduler.jobs.dlq.added")
                .tag("jobType", jobType)
                .register(registry)
                .increment();
    }

    public void recordLeaderElectionWon() {
        leaderElectionCounter.increment();
    }

    public void recordStaleReset(int count) {
        staleResetCounter.increment(count);
    }

    /**
     * Returns a started timer sample. Pass it to stopTimer() after execution.
     * This pattern (start before, stop after) captures wall-clock duration
     * correctly even across async thread hops.
     */
    public Timer.Sample startExecutionTimer() {
        return Timer.start(registry);
    }

    /**
     * Stops the timer and records the sample under scheduler.job.execution.duration.
     * Tagged by jobType and outcome so you can query:
     *   "p99 execution time for EMAIL_NOTIFICATION jobs that succeeded"
     */
    public void stopExecutionTimer(Timer.Sample sample, String jobType, boolean success) {
        sample.stop(Timer.builder("scheduler.job.execution.duration")
                .description("End-to-end job execution time including handler logic")
                .tag("jobType", jobType)
                .tag("outcome", success ? "success" : "failure")
                .publishPercentileHistogram()
                .register(registry));
    }
}
