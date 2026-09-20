package com.taskmesh.mcp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * TaskMesh MCP Server
 *
 * Exposes TaskMesh's distributed job scheduler as an MCP (Model Context Protocol) server.
 * AI agents (Claude, GPT-4, Gemini, etc.) can connect via SSE at /sse and use tools to:
 *
 *   - create_job      → Submit background tasks to the distributed worker pool
 *   - get_job         → Poll job status and retrieve results
 *   - list_jobs       → Browse all jobs with optional status filter
 *   - cancel_job      → Terminate queued or running jobs
 *   - retry_job       → Retry a failed job
 *   - list_workers    → Inspect the worker cluster capacity and load
 *
 * This allows AI agents to offload long-running, CPU-intensive, or scheduled work
 * to the TaskMesh infrastructure instead of executing everything synchronously.
 */
@SpringBootApplication
public class TaskmeshMcpApplication {
    public static void main(String[] args) {
        SpringApplication.run(TaskmeshMcpApplication.class, args);
    }
}
