package com.scheduler.service;

import com.scheduler.domain.entity.Job;
import com.scheduler.domain.entity.JobDependency;
import com.scheduler.domain.enums.JobStatus;
import com.scheduler.repository.JobDependencyRepository;
import com.scheduler.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class DagEvaluatorService {

    private final JobDependencyRepository dependencyRepository;
    private final JobRepository jobRepository;

    /**
     * Called by WorkerPoolService immediately after a ONE_TIME job reaches COMPLETED.
     *
     * Algorithm:
     *   1. Find all jobs that declared a dependency on the completed job.
     *   2. For each waiting dependent, fetch its full dependency list.
     *   3. If every dependency has status = COMPLETED, promote the job to SCHEDULED.
     *   4. Jobs with at least one unmet dependency stay WAITING.
     *
     * This runs inside its own transaction so a failure in DAG evaluation
     * never rolls back the parent job's success record.
     */
    @Transactional
    public void evaluateDependents(UUID completedJobId) {
        List<JobDependency> waitingDependents =
                dependencyRepository.findByDependsOnJobId(completedJobId);

        if (waitingDependents.isEmpty()) {
            log.debug("[DAG] No dependents found for completed job: {}", completedJobId);
            return;
        }

        log.info("[DAG] Evaluating {} dependent(s) of completed job: {}",
                waitingDependents.size(), completedJobId);

        for (JobDependency dependentEdge : waitingDependents) {
            Job dependent = dependentEdge.getJob();

            if (dependent.getStatus() != JobStatus.WAITING) {
                // Could already be DEAD, PAUSED, etc. — skip silently.
                continue;
            }

            boolean allSatisfied = allDependenciesMet(dependent.getId());

            if (allSatisfied) {
                dependent.setStatus(JobStatus.SCHEDULED);
                dependent.setNextRunAt(Instant.now());
                jobRepository.save(dependent);
                log.info("[DAG] Job {} unlocked — all dependencies satisfied. Promoting to SCHEDULED.",
                        dependent.getId());
            } else {
                log.debug("[DAG] Job {} still has unmet dependencies — staying WAITING.", dependent.getId());
            }
        }
    }

    /**
     * Returns true only if every job this job depends on has reached COMPLETED.
     * Fetches the full dependency list with a single JOIN query.
     */
    private boolean allDependenciesMet(UUID jobId) {
        List<JobDependency> allDeps = dependencyRepository.findByJobIdWithDependencies(jobId);
        return allDeps.stream()
                .map(d -> d.getDependsOnJob().getStatus())
                .allMatch(s -> s == JobStatus.COMPLETED);
    }

    /**
     * Cycle detection using iterative DFS.
     * Called at job registration time — throws if the proposed dependency set
     * would create a cycle in the existing DAG.
     *
     * Traversal direction: follows "what does this job depend on" edges.
     * If we can reach any job in the proposed dependsOn set by following
     * those edges from itself, there's a cycle.
     *
     * Example cycle: A → B → C → A
     *   When registering A with dependsOn=[B]:
     *   Start at B, find B depends on C, find C depends on A — A is in our dependsOn set → CYCLE.
     */
    public void validateNoCycles(UUID newJobId, List<UUID> dependsOnIds) {
        Set<UUID> proposedDeps = new HashSet<>(dependsOnIds);

        // A job depending on itself is a trivial cycle
        if (proposedDeps.contains(newJobId)) {
            throw new com.scheduler.exception.InvalidJobRequestException(
                    "A job cannot depend on itself: " + newJobId);
        }

        // For each proposed dependency, traverse its dependency subtree.
        // If we encounter newJobId or any job already in our proposed set
        // that creates a back-edge, it's a cycle.
        for (UUID depId : dependsOnIds) {
            Set<UUID> visited = new HashSet<>();
            Deque<UUID> stack = new ArrayDeque<>();
            stack.push(depId);

            while (!stack.isEmpty()) {
                UUID current = stack.pop();
                if (!visited.add(current)) continue;

                List<UUID> transitiveDeps = dependencyRepository.findDependsOnIdsByJobId(current);
                for (UUID transitiveDepId : transitiveDeps) {
                    if (transitiveDepId.equals(newJobId)) {
                        throw new com.scheduler.exception.InvalidJobRequestException(
                                "Dependency cycle detected: adding job " + newJobId +
                                " → " + depId + " would create a cycle");
                    }
                    stack.push(transitiveDepId);
                }
            }
        }
    }
}
