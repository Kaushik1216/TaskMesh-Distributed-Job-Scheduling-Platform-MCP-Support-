package com.taskmesh.api.repository;

import com.taskmesh.common.entity.WorkflowEdge;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface WorkflowEdgeRepository extends JpaRepository<WorkflowEdge, UUID> {
    List<WorkflowEdge> findByWorkflowId(UUID workflowId);
    List<WorkflowEdge> findByWorkflowIdAndToNodeKey(UUID workflowId, String toNodeKey);
    List<WorkflowEdge> findByWorkflowIdAndFromNodeKey(UUID workflowId, String fromNodeKey);
}
