package com.taskmesh.mcp.tools;

import com.taskmesh.mcp.client.TaskmeshApiClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Service;

/**
 * MCP Tool definitions for AI agents to create and manage DAG workflows.
 *
 * Workflows are directed acyclic graphs (DAGs) of jobs with dependency edges.
 * The scheduler resolves the DAG topologically: root nodes run first,
 * downstream nodes are unblocked when all parent nodes complete.
 *
 * Example agentic workflow:
 *   1. Agent calls create_workflow to define a CI/CD pipeline: build → test → deploy
 *   2. TaskMesh validates the DAG (no cycles), creates jobs, queues root nodes
 *   3. Agent polls get_workflow to monitor progress of the entire pipeline
 *   4. If a node fails, the workflow's failure policy kicks in (FAIL_FAST or CONTINUE)
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class WorkflowManagementTools {

    private final TaskmeshApiClient apiClient;

    @Tool(description = """
        Create a DAG (Directed Acyclic Graph) workflow — a pipeline of jobs with dependency ordering.

        Each node is a job with a key, type, priority, and payload.
        Edges define "must complete before" relationships: {"from": "build", "to": "test"}

        The scheduler validates the DAG (rejects cycles), creates all jobs, and queues root nodes
        (nodes with no incoming edges) immediately. Downstream nodes are automatically unblocked
        when ALL parent nodes complete. Parent results are injected into child payloads.

        Failure policies:
          FAIL_FAST: Cancel all remaining nodes on first failure (default)
          CONTINUE:  Skip dependents of the failed node, let independent branches continue

        Parameters:
          nodesJson: JSON array of nodes, e.g. [{"key":"build","type":"BUILD_PROJECT","priority":"HIGH","payload":"{}"}]
          edgesJson: JSON array of edges, e.g. [{"from":"build","to":"test"},{"from":"test","to":"deploy"}]

        Returns the created workflow with all node statuses.
        Use get_workflow() to poll for pipeline completion.
        """)
    public String createWorkflow(
        @ToolParam(description = "Workflow name, e.g. 'CI/CD Pipeline'")
        String name,

        @ToolParam(description = "Optional description of what this workflow does", required = false)
        String description,

        @ToolParam(description = "Failure policy: FAIL_FAST (default) or CONTINUE", required = false)
        String failurePolicy,

        @ToolParam(description = "JSON array of node definitions. Each node: {key, type, priority?, payload?, maxRetries?, timeoutSeconds?}")
        String nodesJson,

        @ToolParam(description = "JSON array of edge definitions. Each edge: {from, to}. Leave empty for independent parallel jobs.", required = false)
        String edgesJson
    ) {
        log.info("[MCP:tool] create_workflow name='{}' failurePolicy={}", name, failurePolicy);
        return apiClient.createWorkflow(name, description, failurePolicy, nodesJson, edgesJson);
    }

    @Tool(description = """
        Get the full status of a workflow DAG, including each node's job status and result.

        Returns:
          - status: PENDING → RUNNING → COMPLETED | FAILED | CANCELLED
          - totalNodes, completedNodes, failedNodes: progress counters
          - nodes[]: each with key, jobId, jobStatus, result, errorMessage
          - edges[]: the dependency structure

        Poll this until workflow status is COMPLETED or FAILED.
        """)
    public String getWorkflow(
        @ToolParam(description = "The workflow ID (UUID) returned by create_workflow.") String workflowId
    ) {
        log.info("[MCP:tool] get_workflow workflowId={}", workflowId);
        return apiClient.getWorkflow(workflowId);
    }

    @Tool(description = """
        List all workflows, optionally filtered by status.
        Filter options: PENDING, RUNNING, COMPLETED, FAILED, CANCELLED
        Leave status empty to get all workflows.
        """)
    public String listWorkflows(
        @ToolParam(description = "Filter by status (optional)", required = false) String status
    ) {
        log.info("[MCP:tool] list_workflows status={}", status);
        return apiClient.listWorkflows(status);
    }

    @Tool(description = """
        Cancel an entire workflow. All non-terminal jobs are cancelled.
        Use this to abort a pipeline that is no longer needed.
        """)
    public String cancelWorkflow(
        @ToolParam(description = "The workflow ID (UUID) to cancel.") String workflowId
    ) {
        log.info("[MCP:tool] cancel_workflow workflowId={}", workflowId);
        return apiClient.cancelWorkflow(workflowId);
    }
}
