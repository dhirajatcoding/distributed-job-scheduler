package com.scheduler.repository;

import com.scheduler.domain.entity.JobDependency;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface JobDependencyRepository extends JpaRepository<JobDependency, UUID> {

    // "Which jobs are waiting on this completed job?"
    // Called by DagEvaluatorService after every successful job completion.
    List<JobDependency> findByDependsOnJobId(UUID dependsOnJobId);

    // "What are all the prerequisites of this job?"
    // Used to check whether ALL dependencies are satisfied before promoting to SCHEDULED.
    @Query("""
            SELECT jd FROM JobDependency jd
            JOIN FETCH jd.dependsOnJob
            WHERE jd.job.id = :jobId
            """)
    List<JobDependency> findByJobIdWithDependencies(@Param("jobId") UUID jobId);

    // Used during cycle detection — raw IDs only for performance
    @Query("SELECT jd.dependsOnJob.id FROM JobDependency jd WHERE jd.job.id = :jobId")
    List<UUID> findDependsOnIdsByJobId(@Param("jobId") UUID jobId);

    boolean existsByJobIdAndDependsOnJobId(UUID jobId, UUID dependsOnJobId);
}
