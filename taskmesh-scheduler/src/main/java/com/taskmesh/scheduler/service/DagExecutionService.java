package com.taskmesh.scheduler.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskmesh.common.entity.*;
import com.taskmesh.common.enums.FailurePolicy;
import com.taskmesh.common.enums.JobStatus;
import com.taskmesh.common.enums.WorkflowStatus;
import com.taskmesh.common.event.JobCreatedEvent;
import com.taskmesh.scheduler.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DAG Execution Engine — the brain of workflow orchestration.
 *
 * Called by JobSchedulerService after a job completes or fails.
 * If the job belongs to a workflow, this service:
 *
 * On COMPLETED:
 *   1. Increment workflow.completedNodes
 *   2. For each downstream node (outgoing edges):
 *      - Check if ALL parent nodes are COMPLETED
 *      - If yes → inject parent results into child payload → set child QUEUED → publish
 *   3. Check if workflow is terminal (all nodes done)
 *
 * On FAILED / DEAD_LETTER:
 *   1. Increment workflow.failedNodes
 *   2. If FAIL_FAST → cancel all remaining jobs → mark workflow FAILED
 *   3. If CONTINUE  → cancel only dependent jobs → continue independent branches
 *   4. Check terminal condition
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DagExecutionService {

    private static final String TOPIC_CREATED = "taskmesh.jobs.created";

    private final JobRepository           jobRepository;
    private final WorkflowRepository      workflowRepository;
    private final WorkflowNodeRepository  nodeRepository;
    private final WorkflowEdgeRepository  edgeRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper            objectMapper;

    /**
     * Called when a workflow node's job completes successfully.
     * Advances the DAG by unblocking downstream nodes whose parents are all done.
     */
    @Transactional
    public void onNodeCompleted(UUID jobId) {
        Job job = jobRepository.findById(jobId).orElse(null);
        if (job == null || job.getWorkflowId() == null) return;

        UUID workflowId = job.getWorkflowId();
        String nodeKey = job.getWorkflowNodeKey();

        Workflow workflow = workflowRepository.findById(workflowId).orElse(null);
        if (workflow == null || workflow.getStatus() != WorkflowStatus.RUNNING) return;

        // Increment completed count
        workflow.setCompletedNodes(workflow.getCompletedNodes() + 1);
        workflowRepository.save(workflow);

        log.info("[DAG] Node COMPLETED workflow={} node={} ({}/{})",
            workflowId, nodeKey, workflow.getCompletedNodes(), workflow.getTotalNodes());

        // Find downstream nodes via outgoing edges
        List<WorkflowEdge> outEdges = edgeRepository.findByWorkflowIdAndFromNodeKey(workflowId, nodeKey);

        for (WorkflowEdge edge : outEdges) {
            String targetKey = edge.getToNodeKey();
            tryUnblockNode(workflowId, targetKey);
        }

        // Check terminal
        checkWorkflowTerminal(workflow);
    }

    /**
     * Called when a workflow node's job fails or goes to dead letter.
     * Applies the workflow's failure policy.
     */
    @Transactional
    public void onNodeFailed(UUID jobId) {
        Job job = jobRepository.findById(jobId).orElse(null);
        if (job == null || job.getWorkflowId() == null) return;

        UUID workflowId = job.getWorkflowId();
        String nodeKey = job.getWorkflowNodeKey();

        Workflow workflow = workflowRepository.findById(workflowId).orElse(null);
        if (workflow == null || workflow.getStatus() != WorkflowStatus.RUNNING) return;

        // Increment failed count
        workflow.setFailedNodes(workflow.getFailedNodes() + 1);
        workflowRepository.save(workflow);

        log.warn("[DAG] Node FAILED workflow={} node={} policy={}",
            workflowId, nodeKey, workflow.getFailurePolicy());

        if (workflow.getFailurePolicy() == FailurePolicy.FAIL_FAST) {
            // Cancel ALL remaining jobs in the workflow
            cancelRemainingJobs(workflowId);
            workflow.setStatus(WorkflowStatus.FAILED);
            workflow.setCompletedAt(Instant.now());
            workflowRepository.save(workflow);
            log.warn("[DAG] Workflow FAILED (FAIL_FAST) id={}", workflowId);
        } else {
            // CONTINUE policy: cancel only transitive dependents of the failed node
            cancelTransitiveDependents(workflowId, nodeKey);
            // Check if workflow is terminal now
            checkWorkflowTerminal(workflow);
        }
    }

    /**
     * Try to unblock a target node by checking if ALL its parent jobs are COMPLETED.
     * If yes, inject parent results and queue the node.
     */
    private void tryUnblockNode(UUID workflowId, String targetKey) {
        // Find all incoming edges to this target
        List<WorkflowEdge> inEdges = edgeRepository.findByWorkflowIdAndToNodeKey(workflowId, targetKey);

        // Get the target node's job
        WorkflowNode targetNode = nodeRepository.findByWorkflowIdAndNodeKey(workflowId, targetKey).orElse(null);
        if (targetNode == null) return;

        Job targetJob = targetNode.getJob();
        // Only unblock if the job is still in CREATED state (blocked)
        if (targetJob.getStatus() != JobStatus.CREATED) return;

        // Check if ALL parents are COMPLETED
        Map<String, String> parentResults = new LinkedHashMap<>();
        boolean allParentsDone = true;

        for (WorkflowEdge inEdge : inEdges) {
            WorkflowNode parentNode = nodeRepository
                .findByWorkflowIdAndNodeKey(workflowId, inEdge.getFromNodeKey()).orElse(null);
            if (parentNode == null) { allParentsDone = false; break; }

            Job parentJob = parentNode.getJob();
            if (parentJob.getStatus() != JobStatus.COMPLETED) {
                allParentsDone = false;
                break;
            }
            // Collect parent result
            parentResults.put(inEdge.getFromNodeKey(), parentJob.getResult());
        }

        if (!allParentsDone) return;

        // ── All parents done → Inject parent results & queue this node ──

        // Inject parent results into the child payload
        String enrichedPayload = injectParentResults(targetJob.getPayload(), parentResults);
        targetJob.setPayload(enrichedPayload);
        targetJob.setStatus(JobStatus.QUEUED);
        jobRepository.save(targetJob);

        // Publish to Kafka so scheduler assigns it
        Workflow wf = workflowRepository.findById(workflowId).orElse(null);
        String traceId = wf != null ? wf.getTraceId() : null;

        JobCreatedEvent event = new JobCreatedEvent(
            targetJob.getId(), targetJob.getType(), targetJob.getPriority(),
            targetJob.getPayload(), targetJob.getRequiredCapabilities(),
            targetJob.getMaxRetries(), targetJob.getTimeoutSeconds(),
            null, null, traceId, Instant.now()
        );
        publishEvent(TOPIC_CREATED, targetJob.getId().toString(), event);

        log.info("[DAG] Node UNBLOCKED workflow={} node={} jobId={}", workflowId, targetKey, targetJob.getId());
    }

    /**
     * Inject parent results into the child job's payload as "_parentResults".
     * Example:
     *   Original payload: {"command": "deploy.sh"}
     *   After injection:  {"command": "deploy.sh", "_parentResults": {"build": "...", "test": "..."}}
     */
    private String injectParentResults(String originalPayload, Map<String, String> parentResults) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = objectMapper.readValue(
                originalPayload != null ? originalPayload : "{}", Map.class);
            payload.put("_parentResults", parentResults);
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            log.warn("[DAG] Could not inject parent results: {}", e.getMessage());
            return originalPayload;
        }
    }

    /**
     * Cancel all non-terminal jobs in a workflow (used by FAIL_FAST).
     */
    private void cancelRemainingJobs(UUID workflowId) {
        List<WorkflowNode> nodes = nodeRepository.findByWorkflowId(workflowId);
        int cancelled = 0;
        for (WorkflowNode node : nodes) {
            Job job = node.getJob();
            if (isTerminal(job.getStatus())) continue;
            job.setStatus(JobStatus.CANCELLED);
            jobRepository.save(job);
            cancelled++;
        }
        log.info("[DAG] Cancelled {} remaining jobs in workflow={}", cancelled, workflowId);
    }

    /**
     * Cancel only the transitive dependents of a failed node (used by CONTINUE policy).
     * BFS from the failed node through outgoing edges.
     */
    private void cancelTransitiveDependents(UUID workflowId, String failedNodeKey) {
        Queue<String> queue = new LinkedList<>();
        queue.add(failedNodeKey);
        Set<String> visited = new HashSet<>();

        while (!queue.isEmpty()) {
            String current = queue.poll();
            if (!visited.add(current)) continue;

            // Don't cancel the failed node itself (it's already FAILED/DEAD_LETTER)
            if (!current.equals(failedNodeKey)) {
                nodeRepository.findByWorkflowIdAndNodeKey(workflowId, current).ifPresent(node -> {
                    Job job = node.getJob();
                    if (!isTerminal(job.getStatus())) {
                        job.setStatus(JobStatus.CANCELLED);
                        jobRepository.save(job);
                        log.info("[DAG] Cancelled dependent node={} in workflow={}", current, workflowId);
                    }
                });
            }

            // Continue to downstream dependents
            List<WorkflowEdge> outEdges = edgeRepository.findByWorkflowIdAndFromNodeKey(workflowId, current);
            for (WorkflowEdge edge : outEdges) {
                queue.add(edge.getToNodeKey());
            }
        }
    }

    /**
     * Check if a workflow has reached a terminal state.
     * Terminal when: completedNodes + failedNodes + cancelledNodes == totalNodes
     */
    private void checkWorkflowTerminal(Workflow workflow) {
        // Reload to get fresh counts
        workflow = workflowRepository.findById(workflow.getId()).orElse(workflow);

        List<WorkflowNode> nodes = nodeRepository.findByWorkflowId(workflow.getId());
        long terminalCount = nodes.stream()
            .map(n -> n.getJob().getStatus())
            .filter(this::isTerminal)
            .count();

        if (terminalCount >= workflow.getTotalNodes()) {
            boolean hasFailures = workflow.getFailedNodes() > 0 ||
                nodes.stream().anyMatch(n -> n.getJob().getStatus() == JobStatus.CANCELLED);

            workflow.setStatus(hasFailures ? WorkflowStatus.FAILED : WorkflowStatus.COMPLETED);
            workflow.setCompletedAt(Instant.now());
            workflowRepository.save(workflow);
            log.info("[DAG] Workflow {} id={} completed={} failed={}",
                workflow.getStatus(), workflow.getId(),
                workflow.getCompletedNodes(), workflow.getFailedNodes());
        }
    }

    private boolean isTerminal(JobStatus status) {
        return status == JobStatus.COMPLETED ||
               status == JobStatus.FAILED ||
               status == JobStatus.DEAD_LETTER ||
               status == JobStatus.CANCELLED;
    }

    private void publishEvent(String topic, String key, Object event) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(event));
        } catch (Exception e) {
            log.error("[DAG] Failed to publish event to topic={} key={}", topic, key, e);
        }
    }
}
