package com.scheduler.domain.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dead_letter_jobs")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeadLetterJob {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_id", nullable = false)
    private Job job;

    // Points to the last failed execution for full error context
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "last_execution_id")
    private JobExecution lastExecution;

    @Column(name = "failure_reason", columnDefinition = "TEXT")
    private String failureReason;

    @Column(name = "failed_at", nullable = false)
    @Builder.Default
    private Instant failedAt = Instant.now();
}
