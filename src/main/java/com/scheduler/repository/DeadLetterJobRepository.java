package com.scheduler.repository;

import com.scheduler.domain.entity.DeadLetterJob;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface DeadLetterJobRepository extends JpaRepository<DeadLetterJob, UUID> {

    List<DeadLetterJob> findAllByOrderByFailedAtDesc();

    boolean existsByJobId(UUID jobId);
}
