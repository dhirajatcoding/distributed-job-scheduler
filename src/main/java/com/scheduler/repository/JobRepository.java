package com.scheduler.repository;

import com.scheduler.domain.entity.Job;
import com.scheduler.domain.enums.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface JobRepository extends JpaRepository<Job, UUID> {

    @Query(value = """
            SELECT * FROM jobs
            WHERE next_run_at <= NOW()
              AND status = 'SCHEDULED'
            ORDER BY next_run_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<Job> findDueJobs(@Param("limit") int limit);

    @Modifying
    @Query("UPDATE Job j SET j.status = :status, j.updatedAt = :now WHERE j.id = :id")
    void updateStatus(@Param("id") UUID id,
                      @Param("status") JobStatus status,
                      @Param("now") Instant now);

    @Modifying
    @Query("""
            UPDATE Job j SET
              j.status       = 'SCHEDULED',
              j.attemptCount = :attemptCount,
              j.nextRunAt    = :nextRunAt,
              j.updatedAt    = :now
            WHERE j.id = :id
            """)
    void scheduleRetry(@Param("id") UUID id,
                       @Param("attemptCount") int attemptCount,
                       @Param("nextRunAt") Instant nextRunAt,
                       @Param("now") Instant now);

    @Modifying
    @Query("""
            UPDATE Job j SET
              j.status       = 'SCHEDULED',
              j.attemptCount = 0,
              j.nextRunAt    = :nextRunAt,
              j.updatedAt    = :now
            WHERE j.id = :id
            """)
    void reschedule(@Param("id") UUID id,
                    @Param("nextRunAt") Instant nextRunAt,
                    @Param("now") Instant now);

    List<Job> findByStatus(JobStatus status);

    // Phase 3: finds RUNNING jobs whose updated_at is older than the lock TTL.
    // Any job in this state had its executing node crash without cleanup.
    // The Redis lock has since expired, so resetting to SCHEDULED is safe.
    @Query("SELECT j FROM Job j WHERE j.status = 'RUNNING' AND j.updatedAt < :threshold")
    List<Job> findStaleRunningJobs(@Param("threshold") Instant threshold);
}
