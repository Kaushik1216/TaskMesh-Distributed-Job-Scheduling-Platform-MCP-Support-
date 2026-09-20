package com.taskmesh.common.event;

import java.time.Instant;
import java.util.UUID;

public record JobCompletedEvent(
    UUID jobId,
    String workerId,
    String result,
    long durationMs,
    String traceId,
    Instant completedAt
) {}
