<p align="center">
  <h1 align="center">TaskMesh</h1>
  <p align="center">
    <strong>Distributed Job Scheduling Platform with AI Agent Support via MCP</strong>
  </p>
  <p align="center">
    A fault-tolerant, event-driven job scheduling system built with Spring Boot, Kafka, and PostgreSQL.<br/>
    Jobs can be submitted by <strong>AI agents</strong> (Claude, Cursor, Antigravity) through the <strong>Model Context Protocol (MCP)</strong>,<br/>
    or by any application through the <strong>REST API</strong>.
  </p>
</p>

---

## Overview

TaskMesh is a production-style distributed job scheduler that allows both **human developers** and **AI agents** to offload long-running, CPU-intensive, or scheduled tasks to a managed worker pool. Instead of blocking on expensive operations, clients submit jobs and poll for results.

**Why TaskMesh?**
- AI agents like Claude and Cursor have strict execution timeouts. TaskMesh lets them delegate heavy work (builds, tests, data processing) to background workers.
- Traditional applications get a reliable, retry-aware job queue with distributed scheduling and capability-based routing.

---

## Architecture

```
                    ┌─────────────────────────────────────────────────────┐
                    │                   Job Submission                    │
                    │                                                     │
                    │   ┌──────────────┐         ┌───────────────────┐   │
                    │   │  REST API    │         │   MCP Server      │   │
                    │   │  (Direct)    │         │   (AI Agents)     │   │
                    │   │              │         │                   │   │
                    │   │  curl, HTTP  │         │  Claude, Cursor   │   │
                    │   │  Postman,    │         │  Antigravity,     │   │
                    │   │  Any Client  │         │  Any MCP Client   │   │
                    │   └──────┬───────┘         └────────┬──────────┘   │
                    │          │                          │              │
                    └──────────┼──────────────────────────┼──────────────┘
                               │                          │
                               ▼                          ▼
                    ┌─────────────────────────────────────────────────────┐
                    │              TaskMesh API Server (:8080)            │
                    │                                                     │
                    │   • Job CRUD & Idempotency    • Worker Registry     │
                    │   • PostgreSQL Persistence    • Kafka Publishing    │
                    │   • Flyway Migrations         • Redis Caching       │
                    └────────────────────┬────────────────────────────────┘
                                         │
                              Kafka: job-events topic
                                         │
                         ┌───────────────┼───────────────┐
                         ▼                               ▼
              ┌─────────────────────┐         ┌─────────────────────┐
              │   Scheduler ×2      │         │   Workers ×3        │
              │   (HA with Leader   │         │                     │
              │    Election)        │         │   worker-1: BUILD,  │
              │                     │         │     RUN_COMMAND      │
              │   • Picks unassigned│         │   worker-2: TEST,   │
              │     jobs from DB    │         │     FILE, DATA      │
              │   • Routes by       │ ──────► │   worker-3: HTTP,   │
              │     capability      │  Kafka  │     NOTIFY, CMD     │
              │   • Redis leader    │         │                     │
              │     election        │         │   • Concurrent exec │
              │                     │         │   • Heartbeat       │
              └─────────────────────┘         │   • Auto-retry      │
                                              └─────────────────────┘
                                                        │
                         ┌──────────────────────────────┘
                         ▼
              ┌─────────────────────────────────────────────────────┐
              │                  Observability                       │
              │                                                     │
              │   Dashboard (:3001)  Prometheus (:9090)  Grafana    │
              │   • Live job table   • Metrics scraping  (:3000)   │
              │   • Worker status    • Actuator endpoints • Graphs  │
              │   • Quick submit     • Alert rules                  │
              └─────────────────────────────────────────────────────┘
```

> **Dual Ingestion**: Jobs enter the system through two equal paths — the **REST API** for direct programmatic access, and the **MCP Server** for AI agent integration. Both paths converge at the same API server, ensuring identical scheduling, routing, and execution behavior regardless of the submission source.

---

## Key Features

| Feature | Description |
|---|---|
| **Dual Job Submission** | Submit jobs via REST API (curl, Postman, any HTTP client) or via MCP (Claude, Cursor, Antigravity) |
| **Capability-Based Routing** | Workers declare capabilities; the scheduler routes jobs to qualified workers only |
| **High Availability Scheduling** | Two scheduler instances with Redis-based leader election — automatic failover |
| **Idempotent Job Submission** | `Idempotency-Key` header prevents duplicate job creation from retried requests |
| **Configurable Retries** | Jobs auto-retry on failure up to `maxRetries` with attempt tracking |
| **Priority Levels** | Four priority tiers: `CRITICAL` > `HIGH` > `NORMAL` > `LOW` |
| **Concurrent Execution** | Each worker processes multiple jobs simultaneously (configurable concurrency) |
| **Live Dashboard** | Real-time web dashboard with job monitoring, worker status, and quick job submission |
| **Distributed Tracing** | Every job gets a unique `traceId` for end-to-end correlation |
| **Event-Driven Architecture** | Kafka-backed event streaming for reliable, decoupled communication |
| **DAG Workflows** | Orchestrate complex multi-step pipelines with dependency graphs, failure policies (`FAIL_FAST`, `CONTINUE`), and automatic parent-to-child data injection |

---

## DAG Workflow Execution

TaskMesh supports orchestrating multi-step execution pipelines using **Directed Acyclic Graphs (DAGs)**.

- **Dependency Graph:** Define jobs as nodes (e.g., `build`, `test`, `deploy`) and dependencies as edges (e.g., `build` -> `test`). The scheduler automatically resolves the topological order.
- **Smart Unblocking:** Root nodes are executed immediately. Downstream nodes wait until ALL their parent nodes complete successfully.
- **Data Injection:** Results from parent jobs are automatically injected into the child job's payload before execution.
- **Failure Policies:**
  - `FAIL_FAST`: If any node fails, the entire workflow and all remaining pending nodes are cancelled immediately.
  - `CONTINUE`: If a node fails, only its downstream dependents are cancelled. Independent parallel branches continue executing.
- **Cycle Detection:** Built-in validation using Kahn's algorithm guarantees that impossible cyclic pipelines are rejected instantly.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3.3.4 |
| AI Integration | Spring AI 1.0.0-M6 (MCP Server) |
| Database | PostgreSQL 16 |
| Message Broker | Apache Kafka 7.6.0 (Confluent) |
| Cache / Leader Election | Redis 7 |
| Schema Migration | Flyway |
| Monitoring | Prometheus + Grafana |
| Dashboard | HTML/CSS/JS (Nginx) |
| Containerization | Docker + Docker Compose |

---

## Prerequisites

- **Docker Desktop** (v4.0+) with Docker Compose
- **Git**

That's it. No local JDK, Maven, or Node.js installation required — everything runs inside Docker containers.

---

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Kaushik1216/TaskMesh-Distributed-Job-Scheduling-Platform-MCP-Support-.git
cd TaskMesh-Distributed-Job-Scheduling-Platform-MCP-Support-
```

### 2. Build the Project

Build all Java microservices inside a Docker container (no local JDK needed):

```bash
docker run --network host --rm \
  -v "$HOME/.m2:/root/.m2" \
  -v "$(pwd):/project" \
  -w /project \
  maven:3.9-eclipse-temurin-21 \
  mvn clean package -DskipTests
```

### 3. Start the Cluster

```bash
docker compose up -d
```

This spins up **13 containers**:

| Service | URL |
|---|---|
| REST API | http://localhost:8080 |
| MCP Server (SSE) | http://localhost:8090/sse |
| Dashboard | http://localhost:3001 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |

### 4. Verify

```bash
# Check all containers are running
docker compose ps

# Health check
curl http://localhost:8080/actuator/health
# → {"status":"UP"}
```

---

## REST API Reference

Base URL: `http://localhost:8080/api/v1`

### Jobs

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/jobs` | Submit a new job |
| `GET` | `/jobs/{jobId}` | Get job status and result |
| `GET` | `/jobs?status={status}` | List jobs (optional status filter) |
| `DELETE` | `/jobs/{jobId}` | Cancel a job |
| `POST` | `/jobs/{jobId}/retry` | Retry a failed job |

### Workers

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/workers` | List all worker nodes |
| `GET` | `/workers/{workerId}` | Get specific worker info |
| `POST` | `/workers/register` | Register a new worker |
| `POST` | `/workers/{workerId}/heartbeat` | Send worker heartbeat |

### Submit a Job (Example)

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: my-unique-key-001" \
  -d '{
    "type": "DATA_PROCESSING",
    "priority": "HIGH",
    "payload": "{\"dataset\": \"user_analytics\", \"records\": 50000}",
    "maxRetries": 3,
    "timeoutSeconds": 300
  }'
```

### Poll for Result

```bash
curl http://localhost:8080/api/v1/jobs/{jobId}
```

### Job Types

| Type | Description |
|---|---|
| `RUN_COMMAND` | Execute a shell command |
| `HTTP_REQUEST` | Make an HTTP API call |
| `FILE_PROCESSING` | Process files or datasets |
| `TEST_EXECUTION` | Run a test suite |
| `BUILD_PROJECT` | Build/compile a project |
| `SEND_NOTIFICATION` | Send email/Slack/webhook notification |
| `DATA_PROCESSING` | Aggregate or transform data |

### Job Lifecycle

```
QUEUED → ASSIGNED → RUNNING → COMPLETED
                            → FAILED → RETRYING → (re-queued)
                                     → DEAD_LETTER (retries exhausted)
                            → CANCELLED
```

---

## MCP Integration (AI Agents)

TaskMesh exposes a **Model Context Protocol (MCP) server** that allows AI agents to submit and manage jobs using natural language tool calls.

### Available MCP Tools

| Tool | Description |
|---|---|
| `createJob` | Submit a new standalone job to the distributed worker pool |
| `getJob` | Poll job status and retrieve results by job ID |
| `listJobs` | Browse all jobs with optional status filter |
| `cancelJob` | Cancel a queued or running job |
| `retryJob` | Retry a failed or dead-letter job |
| `listWorkers` | Inspect worker cluster capacity and load |
| `createWorkflow` | Create a DAG workflow pipeline (automatically creates and orchestrates child jobs) |
| `getWorkflow` | Poll a workflow's overall status, progression, and individual node statuses |
| `listWorkflows` | List all workflows with optional status filter |
| `cancelWorkflow` | Cancel an entire workflow and all its pending jobs |

### Configure in Claude Desktop

Add to `%APPDATA%\Claude\claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "taskmesh": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://127.0.0.1:8090/sse"]
    }
  }
}
```

### Configure in Antigravity

Add to `.agents/mcp_config.json` in your workspace or `~/.gemini/config/mcp_config.json` globally:

```json
{
  "mcpServers": {
    "taskmesh": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://127.0.0.1:8090/sse"]
    }
  }
}
```

### Example AI Agent Workflow

```
Agent: "Run the regression test suite"

1. Agent calls createJob(type="TEST_EXECUTION", priority="HIGH",
                         payload='{"suite": "regression"}')
   → Returns jobId: "abc-123"

2. Agent calls getJob(jobId="abc-123")
   → Status: RUNNING, assignedWorker: worker-2

3. Agent calls getJob(jobId="abc-123")
   → Status: COMPLETED
   → Result: {"status":"PASSED","testsRun":142,"failures":0}

4. Agent: "All 142 regression tests passed with zero failures."
```

---

## Worker Configuration

Workers are configured via environment variables in `docker-compose.yml`:

| Worker | Capabilities | Max Concurrency |
|---|---|---|
| `worker-1` | `RUN_COMMAND`, `BUILD_PROJECT` | 4 |
| `worker-2` | `TEST_EXECUTION`, `FILE_PROCESSING`, `DATA_PROCESSING` | 8 |
| `worker-3` | `RUN_COMMAND`, `TEST_EXECUTION`, `HTTP_REQUEST`, `SEND_NOTIFICATION` | 2 |

The **scheduler** automatically routes each job to a worker that has the matching capability and available capacity.

---

## Project Structure

```
TaskMesh/
├── common/          # Shared entities, enums (Job, Worker, JobType, JobStatus)
├── api/             # REST API server (Spring Boot + JPA + Kafka Producer)
│   ├── controller/           #   JobController, WorkerController
│   ├── service/              #   JobService, WorkerService
│   ├── dto/                  #   CreateJobRequest, JobResponse, WorkerResponse
│   └── config/               #   SecurityConfig, CorsConfig, AppConfig
├── scheduler/       # Job scheduling engine (Kafka Consumer + Redis Leader Election)
│   ├── service/              #   JobSchedulerService, LeaderElectionService
│   └── strategy/             #   CapabilityAwareStrategy
├── worker/          # Job execution nodes (Kafka Consumer + Job Handlers)
│   ├── executor/             #   ConcurrentJobExecutor
│   ├── handler/              #   RunCommandHandler, HttpRequestHandler, etc.
│   └── service/              #   HeartbeatService
├── mcp/             # MCP Server for AI agents (Spring AI + SSE Transport)
│   ├── tools/                #   JobManagementTools (@Tool annotated)
│   └── client/               #   TaskmeshApiClient (HTTP→API bridge)
├── dashboard/       # Live monitoring dashboard (HTML/CSS/JS + Nginx)
├── monitoring/               # Prometheus configuration
├── docker-compose.yml        # Full cluster orchestration (13 containers)
└── pom.xml                   # Parent Maven POM (multi-module)
```

---

## Dashboard

The TaskMesh Dashboard at **http://localhost:3001** provides:

- **Real-Time Job Registry** — Sortable, filterable table of all jobs with status badges
- **Worker Cluster View** — Live worker load %, heartbeat status, active jobs, and capabilities
- **Scheduler Status** — Active leader instance and failover state
- **Quick Job Submit** — Modal form to submit jobs directly from the browser
- **Job Detail Drawer** — Click any job to view full payload, result JSON, error messages, and trace ID
- **Auto-Refresh** — Polls every 5 seconds with a visible countdown timer

---

## Monitoring

| Tool | URL | Purpose |
|---|---|---|
| **Prometheus** | http://localhost:9090 | Metrics scraping from all Spring Boot Actuator endpoints |
| **Grafana** | http://localhost:3000 | Visualization dashboards (login: `admin` / `admin`) |

Spring Boot Actuator exposes metrics at `/actuator/prometheus` on the API, Scheduler, and Worker services.

---

## Stop the Cluster

```bash
docker compose down
```

To also remove persistent volumes (database data):

```bash
docker compose down -v
```

---

## License

This project is for educational and portfolio purposes.

---

<p align="center">
  Built with ☕ Java 21 · Spring Boot · Kafka · PostgreSQL · Spring AI MCP
</p>
