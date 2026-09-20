package com.taskmesh.api.dto;

import com.taskmesh.common.enums.JobPriority;
import com.taskmesh.common.enums.JobType;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class CreateJobRequest {

    @NotNull(message = "Job type is required")
    private JobType type;

    private String payload = "{}";

    private JobPriority priority = JobPriority.NORMAL;

    /** Comma-separated capabilities, e.g. "TEST_EXECUTION,RUN_COMMAND" */
    private String requiredCapabilities;

    private int maxRetries = 3;

    private int timeoutSeconds = 300;

    /** ISO-8601 datetime for delayed execution (optional) */
    private String scheduledAt;

    /** Standard cron expression for recurring jobs (optional) */
    private String cronExpression;
}
