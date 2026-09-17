package com.taskmesh.common.entity;

import com.taskmesh.common.enums.FailurePolicy;
import com.taskmesh.common.enums.WorkflowStatus;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A workflow is a DAG (Directed Acyclic Graph) of jobs.
 * Each node in the DAG maps to one Job. Edges define execution order.
 *
 * Lifecycle:
 *   PENDING → RUNNING (when first root node is queued)
 *           → COMPLETED (all nodes completed)
 *           → FAILED (a node failed under FAIL_FAST, or all runnable nodes done with failures)
 *           → CANCELLED (user cancelled)
 */
@Entity
@Table(name = "workflows")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Workflow {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String name;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private WorkflowStatus status = WorkflowStatus.PENDING;

    @Enumerated(EnumType.STRING)
    @Column(name = "failure_policy", nullable = false)
    @Builder.Default
    private FailurePolicy failurePolicy = FailurePolicy.FAIL_FAST;

    @Column(name = "trace_id")
    private String traceId;

    @Column(name = "total_nodes", nullable = false)
    @Builder.Default
    private int totalNodes = 0;

    @Column(name = "completed_nodes", nullable = false)
    @Builder.Default
    private int completedNodes = 0;

    @Column(name = "failed_nodes", nullable = false)
    @Builder.Default
    private int failedNodes = 0;

    @OneToMany(mappedBy = "workflow", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    @Builder.Default
    private List<WorkflowNode> nodes = new ArrayList<>();

    @OneToMany(mappedBy = "workflow", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    @Builder.Default
    private List<WorkflowEdge> edges = new ArrayList<>();

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private Instant updatedAt = Instant.now();

    @Column(name = "completed_at")
    private Instant completedAt;

    @PreUpdate
    public void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
