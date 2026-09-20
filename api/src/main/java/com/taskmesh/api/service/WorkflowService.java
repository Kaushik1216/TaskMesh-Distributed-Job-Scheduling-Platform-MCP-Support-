package com.taskmesh.api.service;

import com.taskmesh.api.dto.CreateWorkflowRequest;
import com.taskmesh.api.dto.WorkflowResponse;
import com.taskmesh.api.kafka.JobEventPublisher;
import com.taskmesh.api.repository.JobRepository;
import com.taskmesh.api.repository.WorkflowEdgeRepository;
import com.taskmesh.api.repository.WorkflowNodeRepository;
import com.taskmesh.api.repository.WorkflowRepository;
import com.taskmesh.common.entity.*;
import com.taskmesh.common.enums.JobStatus;
import com.taskmesh.common.enums.WorkflowStatus;
import com.taskmesh.common.event.JobCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for creating and managing DAG-based workflows.
 *
 * Workflow creation flow:
 *   1. Validate DAG (cycle detection via Kahn's algorithm)
 *   2. Persist Workflow + Nodes + Edges
 *   3. Create a Job entity for each node (status = CREATED, blocked)
 *   4. Identify root nodes (no incoming edges)
 *   5. Set root jobs → QUEUED (ready for scheduling)
 *   6. Publish JobCreatedEvent for each root job
 *
 * The scheduler picks up the root jobs. When they complete,
 * DagExecutionService (in the scheduler module) unblocks downstream nodes.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class WorkflowService {

    private final WorkflowRepository workflowRepository;
    private final WorkflowNodeRepository nodeRepository;
    private final WorkflowEdgeRepository edgeRepository;
    private final JobRepository jobRepository;
    private final JobEventPublisher eventPublisher;

    @Transactional
    public WorkflowResponse createWorkflow(CreateWorkflowRequest request) {
        // 1. Validate node keys are unique
        Set<String> nodeKeys = new HashSet<>();
        for (CreateWorkflowRequest.NodeDef nd : request.getNodes()) {
            if (!nodeKeys.add(nd.getKey())) {
                throw new IllegalArgumentException("Duplicate node key: " + nd.getKey());
            }
        }

        // 2. Validate edge references
        for (CreateWorkflowRequest.EdgeDef ed : request.getEdges()) {
            if (!nodeKeys.contains(ed.getFrom())) {
                throw new IllegalArgumentException("Edge references unknown 'from' node: " + ed.getFrom());
            }
            if (!nodeKeys.contains(ed.getTo())) {
                throw new IllegalArgumentException("Edge references unknown 'to' node: " + ed.getTo());
            }
            if (ed.getFrom().equals(ed.getTo())) {
                throw new IllegalArgumentException("Self-loop detected: " + ed.getFrom());
            }
        }

        // 3. Cycle detection using Kahn's algorithm (BFS topological sort)
        validateNoCycles(nodeKeys, request.getEdges());

        String traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);

        // 4. Create the Workflow entity
        Workflow workflow = Workflow.builder()
            .name(request.getName())
            .description(request.getDescription())
            .status(WorkflowStatus.PENDING)
            .failurePolicy(request.getFailurePolicy())
            .traceId(traceId)
            .totalNodes(request.getNodes().size())
            .build();
        workflow = workflowRepository.save(workflow);

        // 5. Find root nodes (no incoming edges)
        Set<String> nodesWithIncoming = request.getEdges().stream()
            .map(CreateWorkflowRequest.EdgeDef::getTo)
            .collect(Collectors.toSet());
        Set<String> rootNodeKeys = nodeKeys.stream()
            .filter(k -> !nodesWithIncoming.contains(k))
            .collect(Collectors.toSet());

        // 6. Create a Job for each node
        Map<String, Job> jobMap = new LinkedHashMap<>();
        for (CreateWorkflowRequest.NodeDef nd : request.getNodes()) {
            boolean isRoot = rootNodeKeys.contains(nd.getKey());
            Job job = Job.builder()
                .type(nd.getType())
                .status(isRoot ? JobStatus.QUEUED : JobStatus.CREATED)
                .priority(nd.getPriority())
                .payload(nd.getPayload())
                .requiredCapabilities(nd.getRequiredCapabilities())
                .maxRetries(nd.getMaxRetries())
                .timeoutSeconds(nd.getTimeoutSeconds())
                .traceId(traceId)
                .workflowId(workflow.getId())
                .workflowNodeKey(nd.getKey())
                .build();
            job = jobRepository.save(job);
            jobMap.put(nd.getKey(), job);
        }

        // 7. Create WorkflowNode records
        for (Map.Entry<String, Job> entry : jobMap.entrySet()) {
            WorkflowNode node = WorkflowNode.builder()
                .workflow(workflow)
                .nodeKey(entry.getKey())
                .job(entry.getValue())
                .build();
            nodeRepository.save(node);
        }

        // 8. Create WorkflowEdge records
        for (CreateWorkflowRequest.EdgeDef ed : request.getEdges()) {
            WorkflowEdge edge = WorkflowEdge.builder()
                .workflow(workflow)
                .fromNodeKey(ed.getFrom())
                .toNodeKey(ed.getTo())
                .build();
            edgeRepository.save(edge);
        }

        // 9. Mark workflow RUNNING and publish root jobs
        workflow.setStatus(WorkflowStatus.RUNNING);
        workflowRepository.save(workflow);

        for (String rootKey : rootNodeKeys) {
            Job rootJob = jobMap.get(rootKey);
            JobCreatedEvent event = new JobCreatedEvent(
                rootJob.getId(), rootJob.getType(), rootJob.getPriority(),
                rootJob.getPayload(), rootJob.getRequiredCapabilities(),
                rootJob.getMaxRetries(), rootJob.getTimeoutSeconds(),
                null, null, traceId, rootJob.getCreatedAt()
            );
            eventPublisher.publishJobCreated(event);
        }

        log.info("Workflow CREATED id={} name='{}' nodes={} roots={} edges={} traceId={}",
            workflow.getId(), workflow.getName(), workflow.getTotalNodes(),
            rootNodeKeys.size(), request.getEdges().size(), traceId);

        return mapToResponse(workflow, jobMap, request.getEdges());
    }

    /**
     * Kahn's algorithm for cycle detection.
     * If the topological sort doesn't visit all nodes, the graph has a cycle.
     */
    private void validateNoCycles(Set<String> nodeKeys, List<CreateWorkflowRequest.EdgeDef> edges) {
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> adjacency = new HashMap<>();
        nodeKeys.forEach(k -> { inDegree.put(k, 0); adjacency.put(k, new ArrayList<>()); });

        for (CreateWorkflowRequest.EdgeDef e : edges) {
            adjacency.get(e.getFrom()).add(e.getTo());
            inDegree.merge(e.getTo(), 1, Integer::sum);
        }

        Queue<String> queue = new LinkedList<>();
        inDegree.forEach((k, v) -> { if (v == 0) queue.add(k); });

        int processed = 0;
        while (!queue.isEmpty()) {
            String node = queue.poll();
            processed++;
            for (String child : adjacency.get(node)) {
                inDegree.merge(child, -1, Integer::sum);
                if (inDegree.get(child) == 0) queue.add(child);
            }
        }

        if (processed != nodeKeys.size()) {
            throw new IllegalArgumentException(
                "DAG contains a cycle! Topological sort visited " + processed +
                " of " + nodeKeys.size() + " nodes.");
        }
    }

    @Transactional(readOnly = true)
    public Optional<WorkflowResponse> getWorkflow(UUID workflowId) {
        return workflowRepository.findById(workflowId).map(wf -> {
            List<WorkflowNode> nodes = nodeRepository.findByWorkflowId(workflowId);
            List<WorkflowEdge> edges = edgeRepository.findByWorkflowId(workflowId);
            Map<String, Job> jobMap = new LinkedHashMap<>();
            nodes.forEach(n -> jobMap.put(n.getNodeKey(), n.getJob()));

            List<CreateWorkflowRequest.EdgeDef> edgeDefs = edges.stream().map(e -> {
                CreateWorkflowRequest.EdgeDef ed = new CreateWorkflowRequest.EdgeDef();
                ed.setFrom(e.getFromNodeKey());
                ed.setTo(e.getToNodeKey());
                return ed;
            }).toList();

            return mapToResponse(wf, jobMap, edgeDefs);
        });
    }

    @Transactional(readOnly = true)
    public List<WorkflowResponse> listWorkflows(WorkflowStatus status) {
        List<Workflow> workflows = (status != null)
            ? workflowRepository.findByStatusOrderByCreatedAtDesc(status)
            : workflowRepository.findAllByOrderByCreatedAtDesc();

        return workflows.stream().map(wf -> {
            List<WorkflowNode> nodes = nodeRepository.findByWorkflowId(wf.getId());
            List<WorkflowEdge> edges = edgeRepository.findByWorkflowId(wf.getId());
            Map<String, Job> jobMap = new LinkedHashMap<>();
            nodes.forEach(n -> jobMap.put(n.getNodeKey(), n.getJob()));

            List<CreateWorkflowRequest.EdgeDef> edgeDefs = edges.stream().map(e -> {
                CreateWorkflowRequest.EdgeDef ed = new CreateWorkflowRequest.EdgeDef();
                ed.setFrom(e.getFromNodeKey());
                ed.setTo(e.getToNodeKey());
                return ed;
            }).toList();

            return mapToResponse(wf, jobMap, edgeDefs);
        }).toList();
    }

    @Transactional
    public Optional<WorkflowResponse> cancelWorkflow(UUID workflowId) {
        return workflowRepository.findById(workflowId).map(wf -> {
            if (wf.getStatus() == WorkflowStatus.COMPLETED || wf.getStatus() == WorkflowStatus.CANCELLED) {
                throw new IllegalStateException("Cannot cancel workflow in status: " + wf.getStatus());
            }

            // Cancel all non-terminal jobs
            List<WorkflowNode> nodes = nodeRepository.findByWorkflowId(workflowId);
            for (WorkflowNode node : nodes) {
                Job job = node.getJob();
                if (job.getStatus() != JobStatus.COMPLETED &&
                    job.getStatus() != JobStatus.DEAD_LETTER &&
                    job.getStatus() != JobStatus.CANCELLED) {
                    job.setStatus(JobStatus.CANCELLED);
                    jobRepository.save(job);
                }
            }

            wf.setStatus(WorkflowStatus.CANCELLED);
            wf.setCompletedAt(Instant.now());
            workflowRepository.save(wf);
            log.info("Workflow CANCELLED id={} name='{}'", workflowId, wf.getName());

            List<WorkflowEdge> edges = edgeRepository.findByWorkflowId(workflowId);
            Map<String, Job> jobMap = new LinkedHashMap<>();
            nodes.forEach(n -> jobMap.put(n.getNodeKey(), n.getJob()));

            List<CreateWorkflowRequest.EdgeDef> edgeDefs = edges.stream().map(e -> {
                CreateWorkflowRequest.EdgeDef ed = new CreateWorkflowRequest.EdgeDef();
                ed.setFrom(e.getFromNodeKey());
                ed.setTo(e.getToNodeKey());
                return ed;
            }).toList();

            return mapToResponse(wf, jobMap, edgeDefs);
        });
    }

    private WorkflowResponse mapToResponse(Workflow wf, Map<String, Job> jobMap,
                                            List<CreateWorkflowRequest.EdgeDef> edgeDefs) {
        List<WorkflowResponse.NodeDetail> nodeDetails = jobMap.entrySet().stream()
            .map(e -> WorkflowResponse.NodeDetail.builder()
                .key(e.getKey())
                .jobId(e.getValue().getId())
                .jobType(e.getValue().getType().name())
                .jobStatus(e.getValue().getStatus().name())
                .jobPriority(e.getValue().getPriority().name())
                .result(e.getValue().getResult())
                .errorMessage(e.getValue().getErrorMessage())
                .attemptCount(e.getValue().getAttemptCount())
                .build())
            .toList();

        List<WorkflowResponse.EdgeDetail> edgeDetails = edgeDefs.stream()
            .map(e -> WorkflowResponse.EdgeDetail.builder()
                .from(e.getFrom()).to(e.getTo()).build())
            .toList();

        return WorkflowResponse.builder()
            .workflowId(wf.getId())
            .name(wf.getName())
            .description(wf.getDescription())
            .status(wf.getStatus())
            .failurePolicy(wf.getFailurePolicy())
            .traceId(wf.getTraceId())
            .totalNodes(wf.getTotalNodes())
            .completedNodes(wf.getCompletedNodes())
            .failedNodes(wf.getFailedNodes())
            .nodes(nodeDetails)
            .edges(edgeDetails)
            .createdAt(wf.getCreatedAt())
            .updatedAt(wf.getUpdatedAt())
            .completedAt(wf.getCompletedAt())
            .build();
    }
}
