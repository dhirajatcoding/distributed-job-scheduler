package com.scheduler.repository;

import com.scheduler.domain.entity.JobExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface JobExecutionRepository extends JpaRepository<JobExecution, UUID> {

    // Most recent executions first — the API exposes this as the execution history
    List<JobExecution> findByJobIdOrderByStartedAtDesc(UUID jobId);

    // Latest single execution — used to link the DLQ entry to its cause
    JobExecution findTopByJobIdOrderByStartedAtDesc(UUID jobId);
}
