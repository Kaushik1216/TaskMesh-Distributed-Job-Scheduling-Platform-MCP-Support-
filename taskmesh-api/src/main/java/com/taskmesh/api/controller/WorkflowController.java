package com.taskmesh.api.controller;

import com.taskmesh.api.dto.CreateWorkflowRequest;
import com.taskmesh.api.dto.WorkflowResponse;
import com.taskmesh.api.service.WorkflowService;
import com.taskmesh.common.enums.WorkflowStatus;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * REST API for DAG workflow lifecycle management.
 *
 * POST   /api/v1/workflows         → Submit a new DAG workflow
 * GET    /api/v1/workflows         → List workflows (optional ?status= filter)
 * GET    /api/v1/workflows/{id}    → Get workflow with full node detail
 * DELETE /api/v1/workflows/{id}    → Cancel entire workflow
 */
@RestController
@RequestMapping("/api/v1/workflows")
@RequiredArgsConstructor
@Slf4j
public class WorkflowController {

    private final WorkflowService workflowService;

    @PostMapping
    public ResponseEntity<WorkflowResponse> createWorkflow(
            @Valid @RequestBody CreateWorkflowRequest request) {
        log.info("POST /api/v1/workflows name='{}' nodes={} edges={}",
            request.getName(), request.getNodes().size(), request.getEdges().size());
        try {
            WorkflowResponse response = workflowService.createWorkflow(request);
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (IllegalArgumentException e) {
            log.warn("Workflow validation failed: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/{workflowId}")
    public ResponseEntity<WorkflowResponse> getWorkflow(
            @PathVariable(name = "workflowId") UUID workflowId) {
        return workflowService.getWorkflow(workflowId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<WorkflowResponse>> listWorkflows(
            @RequestParam(name = "status", required = false) WorkflowStatus status) {
        return ResponseEntity.ok(workflowService.listWorkflows(status));
    }

    @DeleteMapping("/{workflowId}")
    public ResponseEntity<WorkflowResponse> cancelWorkflow(
            @PathVariable(name = "workflowId") UUID workflowId) {
        return workflowService.cancelWorkflow(workflowId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
}
