package com.taskmesh.mcp.tools;

import com.taskmesh.mcp.client.TaskmeshApiClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Service;

/**
 * MCP Tool definitions for AI agents to interact with TaskMesh.
 *
 * The @Tool annotation marks methods as MCP tools discoverable by AI agents.
 * AI clients (Claude Desktop, Cursor, etc.) connect via SSE at /sse and
 * can call these tools using the MCP protocol.
 *
 * Example AI agent workflow:
 *   1. Agent calls create_job(type="TEST_EXECUTION", priority="HIGH", ...)
 *   2. TaskMesh queues, schedules, and routes the job to a capable worker
 *   3. Agent periodically calls get_job(jobId) to check status
 *   4. Once COMPLETED, agent reads the result and continues its task
 *
 * This pattern lets AI agents delegate long-running work to the distributed
 * worker pool instead of blocking or timing out.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class JobManagementTools {

    private final TaskmeshApiClient apiClient;

    @Tool(description = """
        Submit a new job to the TaskMesh distributed scheduler.
        The job will be queued, assigned to a capable worker, and executed asynchronously.

        Supported job types:
          RUN_COMMAND       - Execute a shell command
          HTTP_REQUEST      - Make an HTTP API call
          FILE_PROCESSING   - Process a file or dataset
          TEST_EXECUTION    - Run a test suite
          BUILD_PROJECT     - Build/compile a project
          SEND_NOTIFICATION - Send email/Slack notification
          DATA_PROCESSING   - Aggregate or transform data

        Priority levels (highest to lowest): CRITICAL, HIGH, NORMAL, LOW

        Returns the created job object with its jobId and initial status=QUEUED.
        Use the returned jobId with get_job() to poll for completion.
        """)
    public String createJob(
        @ToolParam(description = "Job type. One of: RUN_COMMAND, HTTP_REQUEST, FILE_PROCESSING, TEST_EXECUTION, BUILD_PROJECT, SEND_NOTIFICATION, DATA_PROCESSING")
        String type,

        @ToolParam(description = "Priority: CRITICAL, HIGH, NORMAL, or LOW. Defaults to NORMAL.", required = false)
        String priority,

        @ToolParam(description = "Job payload as a JSON string with job-specific parameters.", required = false)
        String payload,

        @ToolParam(description = "Required worker capabilities (comma-separated), e.g. 'TEST_EXECUTION'. Leave empty for any worker.", required = false)
        String requiredCapabilities,

        @ToolParam(description = "Maximum retry attempts on failure. Default: 3", required = false)
        int maxRetries,

        @ToolParam(description = "Job timeout in seconds. Default: 300", required = false)
        int timeoutSeconds
    ) {
        log.info("[MCP:tool] create_job type={} priority={}", type, priority);
        return apiClient.createJob(type, priority, payload, requiredCapabilities, maxRetries, timeoutSeconds);
    }

    @Tool(description = """
        Get the current status and result of a job by its ID.

        Returns job details including:
          - status: QUEUED → ASSIGNED → RUNNING → COMPLETED | FAILED → RETRYING | DEAD_LETTER | CANCELLED
          - assignedWorkerId: which worker is executing it
          - result: JSON result string (when COMPLETED)
          - errorMessage: failure reason (when FAILED/DEAD_LETTER)
          - attemptCount: how many times execution has been attempted
          - traceId: correlation ID for distributed tracing

        Poll this until status is COMPLETED, FAILED, or DEAD_LETTER.
        """)
    public String getJob(
        @ToolParam(description = "The job ID (UUID) returned by create_job.") String jobId
    ) {
        log.info("[MCP:tool] get_job jobId={}", jobId);
        return apiClient.getJob(jobId);
    }

    @Tool(description = """
        List all jobs, optionally filtered by status.
        Useful for monitoring the queue and understanding system load.

        Filter options: QUEUED, ASSIGNED, RUNNING, COMPLETED, FAILED, RETRYING, DEAD_LETTER, CANCELLED
        Leave status empty to get all jobs (returned in descending creation order).
        """)
    public String listJobs(
        @ToolParam(description = "Filter by status (optional). E.g. RUNNING, FAILED. Leave empty for all jobs.", required = false)
        String status
    ) {
        log.info("[MCP:tool] list_jobs status={}", status);
        return apiClient.listJobs(status);
    }

    @Tool(description = """
        Cancel a job that is QUEUED, ASSIGNED, or RUNNING.
        - QUEUED jobs are cancelled immediately.
        - RUNNING jobs: the worker will detect the cancellation and stop.
        - COMPLETED or DEAD_LETTER jobs cannot be cancelled.
        """)
    public String cancelJob(
        @ToolParam(description = "The job ID (UUID) to cancel.") String jobId
    ) {
        log.info("[MCP:tool] cancel_job jobId={}", jobId);
        return apiClient.cancelJob(jobId);
    }

    @Tool(description = """
        Retry a FAILED or DEAD_LETTER job.
        The job will be re-queued and scheduled for re-execution.
        The attempt counter is NOT reset — use this for manual intervention after investigating failures.
        """)
    public String retryJob(
        @ToolParam(description = "The job ID (UUID) to retry.") String jobId
    ) {
        log.info("[MCP:tool] retry_job jobId={}", jobId);
        return apiClient.retryJob(jobId);
    }

    @Tool(description = """
        List all registered worker nodes in the TaskMesh cluster.

        Returns each worker's:
          - workerId:       unique worker name
          - capabilities:   job types this worker can handle
          - maxConcurrency: max simultaneous jobs
          - activeJobs:     currently executing jobs
          - loadPercent:    current utilization (activeJobs/maxConcurrency * 100)
          - status:         ACTIVE or DEAD
          - lastHeartbeat:  timestamp of last heartbeat

        Use this to understand cluster capacity before submitting large batches of jobs.
        """)
    public String listWorkers() {
        log.info("[MCP:tool] list_workers");
        return apiClient.listWorkers();
    }
}
