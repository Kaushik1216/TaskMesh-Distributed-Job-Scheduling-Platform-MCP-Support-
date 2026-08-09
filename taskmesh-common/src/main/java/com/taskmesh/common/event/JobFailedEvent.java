package com.taskmesh.common.event;

import java.time.Instant;
import java.util.UUID;

public record JobFailedEvent(
    UUID jobId,
    String workerId,
    String errorMessage,
    int attemptCount,
    int maxRetries,
    boolean willRetry,
    String traceId,
    Instant failedAt
) {}
