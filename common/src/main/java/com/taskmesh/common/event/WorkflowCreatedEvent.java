package com.taskmesh.common.event;

import com.taskmesh.common.enums.FailurePolicy;

import java.time.Instant;
import java.util.UUID;

public record WorkflowCreatedEvent(
    UUID workflowId,
    String name,
    int totalNodes,
    FailurePolicy failurePolicy,
    String traceId,
    Instant createdAt
) {}
