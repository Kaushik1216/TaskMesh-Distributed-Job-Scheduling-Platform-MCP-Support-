Yes. I would **remove the AI inference completely** and reposition the project around a much cleaner problem:

> **A distributed job scheduling platform that allows applications and AI agents to submit, schedule, monitor, retry, and coordinate background work through APIs and MCP.**

This is actually a stronger project for a **Java Backend + Distributed Systems + AI Agent infrastructure** resume.

# Final Project: Distributed Job Scheduler with MCP

### Core idea

```text
                    Applications
                         |
                         |
                    REST API
                         |
                         ▼
              ┌──────────────────┐
              │  Job Scheduler   │
              └────────┬─────────┘
                       |
                 Kafka / Queue
                       |
          ┌────────────┼────────────┐
          ▼            ▼            ▼
       Worker-1     Worker-2     Worker-3
          |            |            |
          └────────────┼────────────┘
                       |
                    Results


                  AI Agents
                     |
                     ▼
                MCP Server
                     |
                     ▼
                Job Scheduler
```

The **AI agent does not execute the work itself**. It uses your platform as an external execution/scheduling system.

---

# 1. The actual problem

Imagine an AI coding agent.

It needs to perform:

```text
Run tests
Build project
Run static analysis
Generate documentation
Deploy service
Process files
Run benchmark
Schedule recurring task
```

Instead of the agent doing everything synchronously, it can tell your platform:

> "Schedule this task and execute it when a worker is available."

For example:

```text
AI Agent
   |
   | MCP: create_job
   ↓
Distributed Scheduler
   |
   ↓
Queue
   |
   ↓
Worker
   |
   ↓
Execute task
```

The agent can later ask:

```text
get_job_status(job_id)
```

or:

```text
cancel_job(job_id)
```

This gives AI agents a **general-purpose execution and scheduling infrastructure**.

---

# 2. What makes this distributed?

The platform should support multiple independent scheduler/worker instances.

```text
                 Kafka
                   |
       ┌───────────┼───────────┐
       ↓           ↓           ↓
 Scheduler-1  Scheduler-2  Scheduler-3
       |           |           |
       └───────────┼───────────┘
                   |
            Worker Cluster
       ┌───────────┼───────────┐
       ↓           ↓           ↓
    Worker-1   Worker-2   Worker-3
```

All of this runs on your laptop using Docker.

---

# 3. The most important distinction

Don't make the project:

> "A REST API that puts jobs into Kafka."

That's too basic.

Your platform should answer:

### Given 1000 jobs and 10 workers:

**Which worker should execute each job?**

And:

### Given a worker crashes:

**How does the system recover its jobs?**

And:

### Given an AI agent submits the same request twice:

**How do you prevent duplicate execution?**

And:

### Given 100 high-priority jobs and 10,000 low-priority jobs:

**How do you prevent starvation while respecting priority?**

Those are your actual distributed-systems problems.

---

# 4. Core Job Model

Every job should have something like:

```json
{
  "jobId": "job_123",
  "type": "RUN_TESTS",
  "payload": {
    "repository": "my-project",
    "branch": "main"
  },
  "priority": "HIGH",
  "scheduledAt": "2026-08-08T10:00:00Z",
  "maxRetries": 3,
  "timeoutSeconds": 300
}
```

Job lifecycle:

```text
CREATED
   ↓
QUEUED
   ↓
SCHEDULED
   ↓
ASSIGNED
   ↓
RUNNING
   ↓
COMPLETED
```

Failure:

```text
RUNNING
   ↓
FAILED
   ↓
RETRYING
   ↓
QUEUED
```

Permanent failure:

```text
FAILED
   ↓
DEAD_LETTER
```

---

# 5. Job Types

Don't hard-code only one type.

Create a generic job model.

Examples:

```text
RUN_COMMAND
HTTP_REQUEST
FILE_PROCESSING
TEST_EXECUTION
BUILD_PROJECT
SEND_NOTIFICATION
DATA_PROCESSING
```

You can later add:

```text
AI_AGENT_TASK
```

without changing the scheduler.

---

# 6. Worker Architecture

Workers advertise their capabilities.

Worker 1:

```json
{
  "workerId": "worker-1",
  "capabilities": [
    "RUN_COMMAND",
    "BUILD_PROJECT"
  ],
  "maxConcurrency": 4
}
```

Worker 2:

```json
{
  "workerId": "worker-2",
  "capabilities": [
    "TEST_EXECUTION",
    "FILE_PROCESSING"
  ],
  "maxConcurrency": 8
}
```

Worker 3:

```json
{
  "workerId": "worker-3",
  "capabilities": [
    "RUN_COMMAND",
    "TEST_EXECUTION"
  ],
  "maxConcurrency": 2
}
```

Now the scheduler can perform **capability-aware scheduling**.

---

# 7. Scheduling Algorithm

Implement multiple strategies.

### Round Robin

```text
W1 → W2 → W3 → W1 → W2 → W3
```

### Least Loaded

```text
W1 = 90%
W2 = 20%
W3 = 50%

        ↓

       W2
```

### Capability + Load

First filter:

```text
Workers capable of TEST_EXECUTION
```

Then select the least-loaded worker.

### Priority

```text
CRITICAL
HIGH
NORMAL
LOW
```

### Aging

Prevent:

```text
LOW priority job
```

from waiting forever because high-priority jobs continuously arrive.

This becomes a legitimate scheduling engine.

---

# 8. Kafka

Kafka handles asynchronous job distribution.

I'd have topics such as:

```text
jobs.created
jobs.scheduled
jobs.assigned
jobs.completed
jobs.failed
jobs.retry
jobs.dead-letter
```

Flow:

```text
API
 ↓
jobs.created
 ↓
Scheduler
 ↓
jobs.assigned
 ↓
Worker
 ↓
jobs.completed
```

---

# 9. Redis

Use Redis for distributed coordination.

### Worker heartbeat

```text
worker:worker-1
TTL = 10 sec
```

Workers refresh this periodically.

If the key expires:

```text
Worker considered dead
```

### Distributed locks

For example:

```text
scheduler:leader
```

Only one scheduler becomes leader.

### Rate limiting

AI agents shouldn't be able to submit unlimited jobs.

```text
agent-123 → 100 jobs/minute
```

---

# 10. Leader Election

Run multiple scheduler instances:

```text
Scheduler-1
Scheduler-2
Scheduler-3
```

All try:

```text
acquire scheduler lock
```

Suppose:

```text
Scheduler-2 → SUCCESS
```

Then:

```text
Scheduler-2 = Leader
```

Others remain standby.

If Scheduler-2 dies:

```text
lock expires
       ↓
Scheduler-1 acquires lock
       ↓
Scheduler-1 becomes leader
```

This is an excellent distributed-systems feature to demonstrate.

---

# 11. Failure Handling

This should be a major part of the project.

Suppose:

```text
Job-123
   ↓
Worker-2
   ↓
Worker crashes
```

Your platform detects:

```text
heartbeat timeout
```

Then:

```text
Job-123
   ↓
mark attempt failed
   ↓
retry
   ↓
Scheduler
   ↓
Worker-1
```

You should support:

* Retry count
* Exponential backoff
* Maximum retries
* Timeout
* Dead-letter queue

---

# 12. Idempotency

This is **mandatory**.

An AI agent might call:

```text
create_job()
```

and never receive the response because of a network failure.

It retries.

Without idempotency:

```text
Job A
Job B
```

Both execute.

With:

```http
Idempotency-Key: abc123
```

your system recognizes:

```text
abc123 → already processed
```

and returns the original job.

This is an excellent real-world backend problem.

---

# 13. MCP Support ⭐⭐⭐⭐⭐

This is what differentiates your project.

Build an **MCP Server** in front of your scheduler.

The AI agent sees tools like:

```text
create_job
get_job
cancel_job
retry_job
list_jobs
schedule_job
list_workers
get_worker_status
```

Conceptually:

```text
             AI Agent
                 |
                 | MCP
                 ▼
          ┌──────────────┐
          │ MCP Server   │
          └───────┬──────┘
                  |
                  ▼
          Scheduler API
                  |
                  ▼
             Job System
```

The MCP server translates agent tool calls into your backend APIs.

---

# 14. Example AI Agent Interaction

An agent needs to run tests.

It could invoke:

```text
create_job(
    type="TEST_EXECUTION",
    payload={
        "repository": "...",
        "branch": "main"
    },
    priority="HIGH"
)
```

Your MCP server:

```text
MCP request
     ↓
Validate
     ↓
Scheduler API
     ↓
Create Job
     ↓
Kafka
     ↓
Worker
```

Returns:

```json
{
  "jobId": "job_8273",
  "status": "QUEUED"
}
```

The agent can then call:

```text
get_job("job_8273")
```

and eventually:

```json
{
  "status": "COMPLETED",
  "result": {
    "exitCode": 0,
    "duration": 142
  }
}
```

---

# 15. MCP Should NOT Bypass Your Scheduler

This is important architecturally.

Don't do:

```text
AI Agent
   ↓
MCP
   ↓
Worker
```

Instead:

```text
AI Agent
   ↓
MCP
   ↓
Scheduler
   ↓
Queue
   ↓
Worker
```

MCP is simply an **AI-friendly control interface**.

Your distributed scheduler remains the core system.

---

# 16. Recurring Jobs

Support:

```text
Every 5 minutes
Every hour
Every day at 2 AM
Cron expression
```

Example:

```text
schedule_job(
    type="BACKUP",
    cron="0 0 * * *"
)
```

The scheduler creates execution instances.

---

# 17. Dependencies / DAG Jobs ⭐⭐⭐⭐

This is a very good advanced feature.

An agent might need:

```text
Build
  ↓
Test
  ↓
Deploy
```

Represent it as:

```text
        Build
       /     \
      ↓       ↓
   Unit Test Integration Test
       \       /
        ↓     ↓
         Deploy
```

The scheduler executes a task only when its dependencies have completed.

This turns your project into a lightweight workflow engine.

---

# 18. Resource Constraints

Workers advertise:

```text
CPU
Memory
Concurrency
Capabilities
```

Job:

```json
{
  "requiredCapabilities": ["TEST_EXECUTION"],
  "memoryMB": 2048,
  "timeoutSeconds": 300
}
```

Scheduler finds a compatible worker.

This makes scheduling substantially more interesting.

---

# 19. Job Cancellation

Agent:

```text
cancel_job(job_123)
```

Your platform:

```text
QUEUED
    ↓
CANCELLED
```

If running:

```text
RUNNING
    ↓
Worker receives cancellation
    ↓
Terminate task
    ↓
CANCELLED
```

---

# 20. Security

MCP should not give an agent unlimited power.

Implement:

```text
API Keys / JWT
```

and permissions:

```text
agent-A:
  create_job
  get_job
  cancel_own_job

admin:
  cancel_any_job
  manage_workers
  manage_schedules
```

This is particularly important because your MCP interface exposes **real execution capabilities**.

---

# 21. Observability

Add:

### Prometheus

Metrics:

```text
jobs_submitted_total
jobs_completed_total
jobs_failed_total
jobs_retried_total

job_queue_size

job_execution_duration

scheduler_latency

worker_utilization

worker_failures
```

### Grafana

Dashboard:

```text
┌─────────────────────────────────────┐
│ Jobs/sec             142            │
│                                     │
│ Queue                 37            │
│                                     │
│ Active Workers         8            │
│                                     │
│ Success Rate         99.2%          │
│                                     │
│ P95 Latency           320ms         │
└─────────────────────────────────────┘
```

---

# 22. Distributed Tracing

Use OpenTelemetry.

Trace:

```text
AI Agent
   ↓
MCP
   ↓
API
   ↓
Kafka
   ↓
Scheduler
   ↓
Worker
   ↓
Result
```

Every job gets:

```text
traceId
jobId
correlationId
```

This will make debugging distributed execution much easier.

---

# 23. Local Deployment

Everything runs on your laptop.

```text
Docker Compose

├── api
├── mcp-server
├── scheduler-1
├── scheduler-2
├── worker-1
├── worker-2
├── worker-3
├── kafka
├── redis
├── postgres
├── prometheus
└── grafana
```

For example:

```text
                 Your Laptop
                      |
       ┌──────────────┴──────────────┐
       │          Docker             │
       │                             │
       │ API                         │
       │ MCP                         │
       │                             │
       │ Scheduler-1                 │
       │ Scheduler-2                 │
       │                             │
       │ Worker-1                    │
       │ Worker-2                    │
       │ Worker-3                    │
       │                             │
       │ Kafka                       │
       │ Redis                       │
       │ PostgreSQL                  │
       │                             │
       │ Prometheus + Grafana        │
       └─────────────────────────────┘
```

You can kill:

```text
worker-2
```

and demonstrate automatic recovery.

---

# 24. Final Technology Stack

### Core

* **Java 21**
* **Spring Boot**
* Spring Data JPA
* Spring Kafka
* Spring Security

### Distributed infrastructure

* **Kafka**
* **Redis**
* **PostgreSQL**

### AI integration

* **MCP Server**
* MCP-compatible AI client/agent

### Observability

* Prometheus
* Grafana
* OpenTelemetry

### Infrastructure

* Docker
* Docker Compose

### Testing

* JUnit 5
* Mockito
* Testcontainers
* k6

---

# 25. Final Architecture

This is the architecture I would freeze:

```text
                        ┌──────────────┐
                        │   AI Agent   │
                        └──────┬───────┘
                               │
                              MCP
                               │
                        ┌──────▼───────┐
                        │  MCP Server  │
                        └──────┬───────┘
                               │
                               ▼
                        ┌───────────────┐
                        │ API Gateway   │
                        └───────┬───────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │ Job Service     │
                       │ Spring Boot     │
                       └────────┬────────┘
                                │
                         PostgreSQL
                                │
                                ▼
                         Kafka / Queue
                                │
                                ▼
                  ┌────────────────────────┐
                  │ Distributed Scheduler  │
                  └────────────┬───────────┘
                               │
                         Redis State
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
         Worker-1         Worker-2         Worker-3
         CPU/4 jobs       CPU/8 jobs       CPU/2 jobs
              │                │                │
              └────────────────┼────────────────┘
                               │
                               ▼
                         Result Store


      Prometheus ← Services → OpenTelemetry → Grafana
```

---

# 26. Scope Priorities

I would divide the final project into:

### 🔴 Must Have

* Java + Spring Boot
* Job API
* Job state machine
* Kafka
* Multiple workers
* Worker registration
* Heartbeats
* Capability-based scheduling
* Priority scheduling
* PostgreSQL
* Redis
* Retry + exponential backoff
* DLQ
* Idempotency
* Job cancellation
* Docker Compose
* MCP server
* MCP tools for job management

### 🟠 Strongly Recommended

* Leader election
* Scheduled/cron jobs
* Resource-aware scheduling
* Job dependencies/DAG
* Rate limiting
* Prometheus
* Grafana
* Integration tests with Testcontainers
* Load testing

### 🟡 Advanced

* OpenTelemetry
* Dynamic worker registration
* Fair scheduling/aging
* Distributed tracing
* Kubernetes deployment
* Autoscaling workers

### 🟢 Don't bother initially

* Complex frontend
* AI model integration
* LLM implementation
* Custom database
* Cloud deployment
* Fancy UI

---

# 27. The project story for your resume

The final story becomes very clean:

> **Distributed Job Scheduling Platform**
> A fault-tolerant distributed execution platform that enables applications and AI agents to schedule and orchestrate background workloads through REST and MCP interfaces.

And your eventual resume bullets could be along the lines of:

> • Designed a distributed job scheduling platform using **Java, Spring Boot, Kafka, Redis and PostgreSQL**, supporting priority-based, capability-aware scheduling across dynamically registered worker nodes.

> • Implemented **worker heartbeats, leader election, idempotent execution, exponential-backoff retries and dead-letter queues** to provide fault-tolerant job execution under worker and network failures.

> • Built an **MCP interface for AI agents** to create, schedule, monitor and cancel jobs, enabling agents to delegate long-running tasks to a distributed worker pool.

> • Added **Prometheus/Grafana observability and load testing** to measure scheduling latency, throughput, queue depth and worker utilization under concurrent workloads.

That is a considerably stronger project than "AI inference platform" for your stated goal because the **core engineering problem is distributed scheduling**, while MCP gives it a modern AI-agent angle without turning the project into another AI/LLM demo.
