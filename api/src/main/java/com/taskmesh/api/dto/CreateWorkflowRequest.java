package com.taskmesh.api.dto;

import com.taskmesh.common.enums.FailurePolicy;
import com.taskmesh.common.enums.JobPriority;
import com.taskmesh.common.enums.JobType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.List;

@Data
public class CreateWorkflowRequest {

    @NotBlank(message = "Workflow name is required")
    private String name;

    private String description;

    private FailurePolicy failurePolicy = FailurePolicy.FAIL_FAST;

    @NotEmpty(message = "At least one node is required")
    @Size(max = 50, message = "Maximum 50 nodes per workflow")
    @Valid
    private List<NodeDef> nodes;

    private List<EdgeDef> edges = List.of();

    @Data
    public static class NodeDef {
        @NotBlank(message = "Node key is required")
        private String key;

        @NotNull(message = "Job type is required")
        private JobType type;

        private JobPriority priority = JobPriority.NORMAL;

        private String payload = "{}";

        private String requiredCapabilities;

        private int maxRetries = 3;

        private int timeoutSeconds = 300;
    }

    @Data
    public static class EdgeDef {
        @NotBlank(message = "Edge 'from' is required")
        private String from;

        @NotBlank(message = "Edge 'to' is required")
        private String to;
    }
}
