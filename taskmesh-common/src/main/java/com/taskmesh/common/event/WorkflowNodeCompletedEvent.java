package com.taskmesh.common.event;

import java.time.Instant;
import java.util.UUID;

public record WorkflowNodeCompletedEvent(
    UUID workflowId,
    String nodeKey,
    UUID jobId,
    String result,
    Instant completedAt
) {}
