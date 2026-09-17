package com.taskmesh.scheduler.repository;

import com.taskmesh.common.entity.WorkflowNode;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface WorkflowNodeRepository extends JpaRepository<WorkflowNode, UUID> {
    List<WorkflowNode> findByWorkflowId(UUID workflowId);
    Optional<WorkflowNode> findByWorkflowIdAndNodeKey(UUID workflowId, String nodeKey);
    Optional<WorkflowNode> findByJobId(UUID jobId);
}
