package com.taskmesh.common.event;

import java.time.Instant;
import java.util.UUID;

public record JobAssignedEvent(
    UUID jobId,
    String workerId,
    String traceId,
    Instant assignedAt
) {}
