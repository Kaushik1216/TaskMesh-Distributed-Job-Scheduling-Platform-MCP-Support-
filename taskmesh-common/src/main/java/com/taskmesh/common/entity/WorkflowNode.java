package com.taskmesh.common.entity;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

/**
 * A node in a workflow DAG, linking a nodeKey to a Job.
 * The nodeKey is a human-readable identifier (e.g., "build", "test", "deploy")
 * used in edge definitions to express dependencies.
 */
@Entity
@Table(name = "workflow_nodes",
       uniqueConstraints = @UniqueConstraint(columnNames = {"workflow_id", "node_key"}))
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowNode {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "workflow_id", nullable = false)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Workflow workflow;

    @Column(name = "node_key", nullable = false, length = 100)
    private String nodeKey;

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_id", nullable = false)
    private Job job;
}
