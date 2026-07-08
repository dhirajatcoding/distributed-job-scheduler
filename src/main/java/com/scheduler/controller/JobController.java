package com.scheduler.controller;

import com.scheduler.dto.CreateJobRequest;
import com.scheduler.dto.ExecutionResponse;
import com.scheduler.dto.JobResponse;
import com.scheduler.service.JobService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;
    private final com.scheduler.service.LeaderElectionService leaderElectionService;
    private final com.scheduler.config.NodeIdentityService nodeIdentityService;

    // ─── Job CRUD ──────────────────────────────────────────────────────────────

    @PostMapping("/jobs")
    public ResponseEntity<JobResponse> registerJob(@Valid @RequestBody CreateJobRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(jobService.registerJob(request));
    }

    @GetMapping("/jobs")
    public ResponseEntity<List<JobResponse>> getAllJobs() {
        return ResponseEntity.ok(jobService.getAllJobs());
    }

    @GetMapping("/jobs/{id}")
    public ResponseEntity<JobResponse> getJob(@PathVariable UUID id) {
        return ResponseEntity.ok(jobService.getJob(id));
    }

    @DeleteMapping("/jobs/{id}")
    public ResponseEntity<Void> deleteJob(@PathVariable UUID id) {
        jobService.deleteJob(id);
        return ResponseEntity.noContent().build();
    }

    // ─── Lifecycle Controls ────────────────────────────────────────────────────

    @PatchMapping("/jobs/{id}/pause")
    public ResponseEntity<JobResponse> pauseJob(@PathVariable UUID id) {
        return ResponseEntity.ok(jobService.pauseJob(id));
    }

    @PatchMapping("/jobs/{id}/resume")
    public ResponseEntity<JobResponse> resumeJob(@PathVariable UUID id) {
        return ResponseEntity.ok(jobService.resumeJob(id));
    }

    // Manually triggers a job immediately regardless of its next_run_at.
    // Sets next_run_at = NOW() so the scheduler picks it up on the next poll cycle.
    @PostMapping("/jobs/{id}/trigger")
    public ResponseEntity<JobResponse> triggerNow(@PathVariable UUID id) {
        return ResponseEntity.ok(jobService.triggerNow(id));
    }

    // ─── Execution History ─────────────────────────────────────────────────────

    @GetMapping("/jobs/{id}/executions")
    public ResponseEntity<List<ExecutionResponse>> getExecutionHistory(@PathVariable UUID id) {
        return ResponseEntity.ok(jobService.getExecutionHistory(id));
    }

    // ─── Dead Letter Queue ─────────────────────────────────────────────────────

    @GetMapping("/dlq")
    public ResponseEntity<?> getDlqJobs() {
        return ResponseEntity.ok(jobService.getDlqJobs());
    }

    // Resets attempt_count and re-enqueues a dead job with a fresh retry budget.
    @PostMapping("/dlq/{dlqId}/requeue")
    public ResponseEntity<JobResponse> requeueFromDlq(@PathVariable UUID dlqId) {
        return ResponseEntity.ok(jobService.requeueFromDlq(dlqId));
    }

    // ─── Operational Metrics ───────────────────────────────────────────────────

    /**
     * Lightweight health/status endpoint. In Phase 2, this will also report
     * the current leader node and Redis lock state.
     * Useful for the admin dashboard and for quick curl checks during demos.
     */
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        List<JobResponse> allJobs = jobService.getAllJobs();

        Map<String, Long> byStatus = allJobs.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        j -> j.getStatus().name(),
                        java.util.stream.Collectors.counting()));

        return ResponseEntity.ok(Map.of(
                "timestamp",    Instant.now(),
                "nodeId",       nodeIdentityService.getNodeId(),
                "isLeader",     leaderElectionService.isLeader(),
                "currentLeader", leaderElectionService.getCurrentLeader() != null
                                    ? leaderElectionService.getCurrentLeader() : "none",
                "totalJobs",    allJobs.size(),
                "byStatus",     byStatus,
                "dlqSize",      jobService.getDlqJobs().size()
        ));
    }
}
