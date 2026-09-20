package com.taskmesh.common.event;

import com.taskmesh.common.enums.JobPriority;
import com.taskmesh.common.enums.JobType;

import java.time.Instant;
import java.util.UUID;

public record JobCreatedEvent(
    UUID jobId,
    JobType type,
    JobPriority priority,
    String payload,
    String requiredCapabilities,
    int maxRetries,
    int timeoutSeconds,
    Instant scheduledAt,
    String idempotencyKey,
    String traceId,
    Instant createdAt
) {}
