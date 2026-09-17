package com.taskmesh.api.dto;

import com.taskmesh.common.enums.JobPriority;
import com.taskmesh.common.enums.JobStatus;
import com.taskmesh.common.enums.JobType;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
public class JobResponse {
    private UUID jobId;
    private JobType type;
    private JobStatus status;
    private JobPriority priority;
    private String payload;
    private String requiredCapabilities;
    private int maxRetries;
    private int attemptCount;
    private int timeoutSeconds;
    private String assignedWorkerId;
    private String result;
    private String errorMessage;
    private String traceId;
    private UUID workflowId;
    private String workflowNodeKey;
    private Instant createdAt;
    private Instant updatedAt;
}
