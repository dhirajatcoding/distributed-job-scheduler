package com.scheduler.scheduler;

import com.scheduler.domain.entity.Job;
import com.scheduler.domain.enums.JobStatus;
import com.scheduler.repository.JobRepository;
import com.scheduler.service.LeaderElectionService;
import com.scheduler.service.WorkerPoolService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobSchedulerEngine {

    private final JobRepository jobRepository;
    private final WorkerPoolService workerPoolService;
    private final LeaderElectionService leaderElectionService;  // Phase 2: added

    @Value("${scheduler.batch-size:20}")
    private int batchSize;

    /**
     * Every tick, check leadership first.
     *
     * Non-leader nodes return immediately — they do nothing except participate
     * in the election on the next LeaderElectionService.refreshLeadership() cycle.
     * This means the scheduler overhead on follower nodes is essentially zero.
     *
     * Leader nodes do the full poll → claim → dispatch cycle.
     * Leadership is re-verified on every tick via a live Redis check, not a
     * cached flag — so if this node loses leadership mid-cycle the next tick
     * catches it immediately.
     */
    @Scheduled(fixedDelayString = "${scheduler.poll-interval-ms:5000}")
    public void tick() {
        // Phase 2: guard — only the leader schedules
        if (!leaderElectionService.isLeader()) {
            log.debug("[SCHEDULER] Not the leader — skipping poll cycle. Current leader: {}",
                    leaderElectionService.getCurrentLeader());
            return;
        }

        log.debug("[SCHEDULER] Leader tick starting");
        List<Job> dueJobs = pollAndDispatch();

        if (dueJobs.isEmpty()) {
            log.debug("[SCHEDULER] No due jobs found");
            return;
        }

        log.info("[SCHEDULER] Dispatching {} job(s) to worker pool", dueJobs.size());

        for (Job job : dueJobs) {
            try {
                workerPoolService.executeJob(job);
            } catch (Exception ex) {
                // Thread pool rejection — all workers busy.
                // Job stays RUNNING in DB. Phase 2 improvement: a stale RUNNING
                // job detector (scheduled task) will reset these after a timeout.
                log.error("[SCHEDULER] Failed to dispatch job id={}: {}", job.getId(), ex.getMessage());
            }
        }
    }

    /**
     * Single transaction: fetch due jobs with SKIP LOCKED + flip status to RUNNING.
     * Committing this transaction is what makes the claim visible to all other nodes.
     * After commit, even if this node crashes before executing, the job shows RUNNING
     * and won't be re-dispatched. (Stale RUNNING cleanup = Phase 3.)
     */
    @Transactional
    protected List<Job> pollAndDispatch() {
        List<Job> dueJobs = jobRepository.findDueJobs(batchSize);

        if (!dueJobs.isEmpty()) {
            Instant now = Instant.now();
            for (Job job : dueJobs) {
                jobRepository.updateStatus(job.getId(), JobStatus.RUNNING, now);
                log.info("[SCHEDULER] Claimed job: id={}, type={}, scheduledAt={}",
                        job.getId(), job.getJobType(), job.getNextRunAt());
            }
        }

        return dueJobs;
    }
}
