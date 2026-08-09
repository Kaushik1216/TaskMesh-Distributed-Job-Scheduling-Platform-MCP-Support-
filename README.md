# TaskMesh — Distributed Job Scheduling Platform

> A fault-tolerant distributed execution platform that enables applications and **AI agents** to schedule, monitor, and orchestrate background workloads through REST and MCP interfaces.

## Architecture

```
AI Agent (Claude / GPT-4 / Gemini)
    │ MCP over SSE
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  taskmesh-mcp  (port 8090)  Spring AI MCP Server                    │
│  Tools: create_job, get_job, list_jobs, cancel_job, list_workers    │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ HTTP
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  taskmesh-api  (port 8080)  Spring Boot REST API                    │
│  • Job CRUD  • Idempotent submission  • Worker registration         │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  Kafka: taskmesh.jobs.created
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  taskmesh-scheduler-1 (8081)  ◄─── LEADER (Redis election)          │
│  taskmesh-scheduler-2 (8082)  ◄─── STANDBY (takes over on failure)  │
│                                                                     │
│  Strategy: Capability-Aware + Least-Loaded                          │
│  Features: Priority scheduling, exponential backoff, dead-letter    │
└─────────┬───────────────────────┬───────────────────────────────────┘
          │  Kafka: taskmesh.jobs.assigned
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  worker-1 (8083)  RUN_COMMAND, BUILD_PROJECT          maxConc=4     │
│  worker-2 (8084)  TEST_EXECUTION, FILE_PROCESSING     maxConc=8     │
│  worker-3 (8085)  RUN_COMMAND, TEST_EXECUTION         maxConc=2     │
│                                                                     │
│  ConcurrentJobExecutor: ThreadPoolExecutor + Semaphore + Timeout    │
└─────────────────────────────────────────────────────────────────────┘
          │  Kafka: taskmesh.jobs.completed / failed
          ▼
    PostgreSQL ──── Redis ──── Prometheus ──── Grafana
```

## Key Technical Features

| Feature | Implementation |
|---|---|
| **Distributed Leader Election** | Redis SETNX with TTL renewal — only one scheduler assigns jobs |
| **Capability-Aware Scheduling** | Workers advertise capabilities; jobs matched to capable + least-loaded worker |
| **Priority Queue** | CRITICAL > HIGH > NORMAL > LOW with aging-based scan |
| **Concurrent Execution** | `ThreadPoolExecutor` + `Semaphore` per worker with configurable `maxConcurrency` |
| **Exponential Backoff Retry** | delay = 2^attempt × 2s, up to `maxRetries` |
| **Dead Letter Queue** | Jobs exceeding max retries → `taskmesh.jobs.dead-letter` Kafka topic |
| **Idempotent Job Creation** | `Idempotency-Key` header — same key = same job returned, no duplicates |
| **Worker Heartbeats** | Redis TTL (30s) + PostgreSQL lastHeartbeat; dead workers excluded from scheduling |
| **Job Cancellation** | QUEUED → immediate; RUNNING → worker detects via DB status check |
| **Java MCP Server** | Spring AI MCP (`@Tool`) exposes 6 tools for AI agent integration |
| **Observability** | Micrometer + Prometheus + Grafana for all services |

## Tech Stack

- **Java 21** + **Spring Boot 3.3.4**
- **Apache Kafka** — async job distribution (5 topics)
- **Redis** — leader election, worker heartbeats
- **PostgreSQL 16** — job persistence with Flyway migrations
- **Spring AI 1.0.0** — Java MCP server (`spring-ai-mcp-server-webmvc-spring-boot-starter`)
- **Micrometer + Prometheus + Grafana** — metrics and dashboards
- **Docker Compose** — full local stack

## Quick Start

### Prerequisites
- Java 21+, Maven 3.9+, Docker Desktop

### Build
```bash
cd taskmesh
mvn clean package -DskipTests
```

### Run
```bash
docker-compose up --build
```

Wait ~60s for all services to start, then verify:
```bash
curl http://localhost:8080/actuator/health
```

## API Quick Reference

### Submit a job
```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: my-unique-key-001" \
  -d '{
    "type": "TEST_EXECUTION",
    "priority": "HIGH",
    "payload": "{\"repository\": \"my-project\", \"branch\": \"main\"}",
    "requiredCapabilities": "TEST_EXECUTION",
    "maxRetries": 3,
    "timeoutSeconds": 60
  }'
```

### Poll job status
```bash
curl http://localhost:8080/api/v1/jobs/{jobId}
```

### List active workers
```bash
curl http://localhost:8080/api/v1/workers
```

### Cancel a job
```bash
curl -X DELETE http://localhost:8080/api/v1/jobs/{jobId}
```

## MCP Integration (AI Agents)

Connect any MCP-compatible AI client to `http://localhost:8090/sse`.

**Claude Desktop** — add to `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "taskmesh": {
      "url": "http://localhost:8090/sse"
    }
  }
}
```

Available MCP tools:
- `create_job` — Submit a job to the distributed worker pool
- `get_job` — Get job status and result
- `list_jobs` — Browse all jobs with status filter
- `cancel_job` — Cancel a queued or running job
- `retry_job` — Retry a failed job
- `list_workers` — Inspect worker cluster capacity

## Demo Scenarios

### 1. Distributed Scheduling
```bash
# Submit 10 jobs of different types — observe them distributed across workers
for i in {1..10}; do
  curl -s -X POST http://localhost:8080/api/v1/jobs \
    -H "Content-Type: application/json" \
    -d "{\"type\": \"TEST_EXECUTION\", \"priority\": \"NORMAL\"}" &
done
```

### 2. Leader Election Failover
```bash
# Kill scheduler-1 — scheduler-2 takes over within 15 seconds
docker stop taskmesh-scheduler-1
# Watch the logs of scheduler-2: "Leadership ACQUIRED"
docker logs taskmesh-scheduler-2 -f
```

### 3. Worker Failure Recovery
```bash
# Kill a worker mid-execution — jobs retry on remaining workers
docker stop taskmesh-worker-2
# Jobs assigned to worker-2 retry with exponential backoff
```

### 4. Idempotency Test
```bash
# Submit the same job twice with the same key — only one job created
KEY="test-idem-$(date +%s)"
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Idempotency-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"type": "RUN_COMMAND"}'
# Second call returns the SAME job
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Idempotency-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"type": "RUN_COMMAND"}'
```

## Service URLs

| Service | URL |
|---|---|
| REST API | http://localhost:8080 |
| Scheduler 1 status | http://localhost:8081/api/v1/scheduler/status |
| Scheduler 2 status | http://localhost:8082/api/v1/scheduler/status |
| MCP Server (SSE) | http://localhost:8090/sse |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
# TaskMesh-Distributed-Job-Scheduling-Platform-MCP-Support-
