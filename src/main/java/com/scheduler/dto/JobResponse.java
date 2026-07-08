package com.scheduler.dto;

import com.scheduler.domain.entity.Job;
import com.scheduler.domain.enums.JobStatus;
import com.scheduler.domain.enums.ScheduleType;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
public class JobResponse {

    private UUID id;
    private String name;
    private String jobType;
    private ScheduleType scheduleType;
    private String cronExpr;
    private String payload;
    private JobStatus status;
    private int maxRetries;
    private int attemptCount;
    private Instant nextRunAt;
    private Instant createdAt;
    private Instant updatedAt;

    public static JobResponse from(Job job) {
        return JobResponse.builder()
                .id(job.getId())
                .name(job.getName())
                .jobType(job.getJobType())
                .scheduleType(job.getScheduleType())
                .cronExpr(job.getCronExpr())
                .payload(job.getPayload())
                .status(job.getStatus())
                .maxRetries(job.getMaxRetries())
                .attemptCount(job.getAttemptCount())
                .nextRunAt(job.getNextRunAt())
                .createdAt(job.getCreatedAt())
                .updatedAt(job.getUpdatedAt())
                .build();
    }
}
