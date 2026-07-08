package com.scheduler.dto;

import com.scheduler.domain.entity.JobExecution;
import com.scheduler.domain.enums.ExecutionStatus;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
public class ExecutionResponse {

    private UUID id;
    private UUID jobId;
    private int attemptNumber;
    private ExecutionStatus status;
    private String workerId;
    private Instant startedAt;
    private Instant finishedAt;
    private String errorMessage;

    public static ExecutionResponse from(JobExecution exec) {
        return ExecutionResponse.builder()
                .id(exec.getId())
                .jobId(exec.getJob().getId())
                .attemptNumber(exec.getAttemptNumber())
                .status(exec.getStatus())
                .workerId(exec.getWorkerId())
                .startedAt(exec.getStartedAt())
                .finishedAt(exec.getFinishedAt())
                .errorMessage(exec.getErrorMessage())
                .build();
    }
}
