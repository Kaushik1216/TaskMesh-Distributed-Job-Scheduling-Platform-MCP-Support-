package com.taskmesh.api.dto;

import com.taskmesh.common.enums.FailurePolicy;
import com.taskmesh.common.enums.WorkflowStatus;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Data
@Builder
public class WorkflowResponse {
    private UUID workflowId;
    private String name;
    private String description;
    private WorkflowStatus status;
    private FailurePolicy failurePolicy;
    private String traceId;
    private int totalNodes;
    private int completedNodes;
    private int failedNodes;
    private List<NodeDetail> nodes;
    private List<EdgeDetail> edges;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant completedAt;

    @Data
    @Builder
    public static class NodeDetail {
        private String key;
        private UUID jobId;
        private String jobType;
        private String jobStatus;
        private String jobPriority;
        private String result;
        private String errorMessage;
        private int attemptCount;
    }

    @Data
    @Builder
    public static class EdgeDetail {
        private String from;
        private String to;
    }
}
