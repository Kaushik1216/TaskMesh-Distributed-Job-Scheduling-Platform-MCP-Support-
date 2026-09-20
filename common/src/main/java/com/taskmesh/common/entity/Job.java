package com.taskmesh.common.entity;

import com.taskmesh.common.enums.JobPriority;
import com.taskmesh.common.enums.JobStatus;
import com.taskmesh.common.enums.JobType;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Core job entity representing a unit of distributed work.
 * Persisted in PostgreSQL; status transitions are managed by the scheduler.
 *
 * State machine:
 *   QUEUED -> ASSIGNED -> RUNNING -> COMPLETED
 *                              ↓
 *                           FAILED -> RETRYING -> QUEUED (retry loop)
 *                              ↓
 *                         DEAD_LETTER (after maxRetries exceeded)
 */
@Entity
@Table(name = "jobs")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Job {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobType type;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private JobStatus status = JobStatus.QUEUED;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private JobPriority priority = JobPriority.NORMAL;

    @Column(columnDefinition = "TEXT")
    private String payload;

    @Column(name = "required_capabilities")
    private String requiredCapabilities;

    @Column(name = "scheduled_at")
    private Instant scheduledAt;

    @Column(name = "cron_expression")
    private String cronExpression;

    @Column(name = "max_retries", nullable = false)
    @Builder.Default
    private int maxRetries = 3;

    @Column(name = "attempt_count", nullable = false)
    @Builder.Default
    private int attemptCount = 0;

    @Column(name = "timeout_seconds", nullable = false)
    @Builder.Default
    private int timeoutSeconds = 300;

    @Column(name = "assigned_worker_id")
    private String assignedWorkerId;

    @Column(columnDefinition = "TEXT")
    private String result;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "idempotency_key", unique = true)
    private String idempotencyKey;

    @Column(name = "trace_id")
    private String traceId;

    /** If this job is part of a workflow DAG, the workflow ID. Null for standalone jobs. */
    @Column(name = "workflow_id")
    private UUID workflowId;

    /** The node key within the workflow DAG (e.g., "build", "test"). Null for standalone jobs. */
    @Column(name = "workflow_node_key", length = 100)
    private String workflowNodeKey;

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private Instant updatedAt = Instant.now();

    @PreUpdate
    public void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
