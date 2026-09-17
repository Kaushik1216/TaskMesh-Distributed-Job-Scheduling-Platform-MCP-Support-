package com.taskmesh.api.repository;

import com.taskmesh.common.entity.Workflow;
import com.taskmesh.common.enums.WorkflowStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface WorkflowRepository extends JpaRepository<Workflow, UUID> {
    List<Workflow> findByStatusOrderByCreatedAtDesc(WorkflowStatus status);
    List<Workflow> findAllByOrderByCreatedAtDesc();
}
