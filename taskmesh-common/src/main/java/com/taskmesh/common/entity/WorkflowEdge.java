package com.taskmesh.common.entity;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

/**
 * An edge in a workflow DAG.
 * Represents: fromNodeKey must complete before toNodeKey can start.
 */
@Entity
@Table(name = "workflow_edges",
       uniqueConstraints = @UniqueConstraint(columnNames = {"workflow_id", "from_node_key", "to_node_key"}))
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowEdge {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "workflow_id", nullable = false)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Workflow workflow;

    @Column(name = "from_node_key", nullable = false, length = 100)
    private String fromNodeKey;

    @Column(name = "to_node_key", nullable = false, length = 100)
    private String toNodeKey;
}
