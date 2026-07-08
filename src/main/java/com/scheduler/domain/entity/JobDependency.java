package com.scheduler.domain.entity;

import jakarta.persistence.*;
import lombok.*;
import java.util.UUID;

@Entity
@Table(name = "job_dependencies")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobDependency {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    // The job that is WAITING for something else to complete
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_id", nullable = false)
    private Job job;

    // The job that must reach COMPLETED before the above job can run
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "depends_on_job_id", nullable = false)
    private Job dependsOnJob;
}
