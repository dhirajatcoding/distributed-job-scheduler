# Distributed Job Scheduler

A production-grade distributed job scheduler built with Spring Boot 3.2, PostgreSQL, and Redis.
Guarantees no duplicate job execution across N nodes with leader-elected scheduling, per-job distributed locks, DAG dependency chains, and automatic recovery from node failures.

---

## Problem Statement

Every SaaS application eventually needs reliable scheduled background work — billing cycles, report generation, data sync, notification dispatch. The naive solution is a cron job on one server. That server becomes a single point of failure. When it goes down, jobs don't run. When it comes back up, no one knows which jobs were mid-execution.

This project implements a scheduler that handles N application nodes correctly: only one node dispatches at a time, no job executes twice, failed nodes are detected and their claimed jobs are recovered automatically.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Clients / Dashboard                 │
└────────────────────────┬────────────────────────────────┘
                         │ HTTP
┌────────────────────────▼────────────────────────────────┐
│              Spring Boot Application Cluster             │
│  ┌─────────────┐  ┌──────────────────┐  ┌────────────┐  │
│  │  REST API   │→ │ Scheduler Engine  │→ │ Worker Pool│  │
│  │             │  │  (Leader-elected) │  │  (@Async)  │  │
│  └─────────────┘  └──────────────────┘  └────────────┘  │
└──────────┬─────────────────┬───────────────────┬────────┘
           │                 │                   │
    ┌──────▼──────┐  ┌───────▼──────┐  ┌────────▼──────┐
    │ PostgreSQL  │  │    Redis      │  │  Dead Letter  │
    │ Job & exec  │  │ Locks + leader│  │     Queue     │
    │    store    │  │   election    │  │  (PostgreSQL) │
    └─────────────┘  └──────────────┘  └───────────────┘
```

**Request flow:**
1. Client registers a job via REST API → persisted to PostgreSQL
2. Scheduler Engine (leader node only) polls for due jobs every 5s
3. Due jobs are claimed atomically (`FOR UPDATE SKIP LOCKED`) and set to RUNNING
4. Each job is submitted to the Worker Pool (dedicated `@Async` thread pool)
5. Worker acquires a per-job Redis distributed lock, then executes the handler
6. On success: job rescheduled (CRON) or completed (ONE_TIME); DAG dependents evaluated
7. On failure: exponential backoff retry; after max retries → DLQ
8. Stale detector runs every 60s and resets RUNNING jobs whose lock TTL has expired

---

## Features

- **Exactly-once dispatch** — `SELECT FOR UPDATE SKIP LOCKED` + distributed Redis lock prevents double execution across any number of nodes
- **Leader election** — Redis `SET NX PX` elects one scheduler leader; failover within 15 seconds on crash
- **DAG job chaining** — jobs can declare dependencies; a WAITING job is promoted to SCHEDULED only when all its dependencies reach COMPLETED
- **Automatic stale recovery** — node crash during execution is detected and recovered without operator intervention
- **Exponential backoff retry** — failed jobs retry at 2^attempt seconds (2s, 4s, 8s…), capped at 1 hour
- **Dead letter queue** — jobs exhausting retries are moved to DLQ with full failure context; one-click requeue
- **Micrometer observability** — execution counters, queue depth gauges, p99 duration histograms, all tagged by job type
- **Multi-node Docker Compose** — two app nodes + PostgreSQL + Redis, fully containerised

---

## Architecture Decision Records

### ADR-1 — Redis SETNX for leader election over ZooKeeper/etcd

**Context:** One node must own the scheduler loop to prevent concurrent dispatch.

**Decision:** Use Redis `SET key value NX PX ttl`. The node that wins this race becomes the leader and refreshes its TTL every 5 seconds. If it crashes, the TTL (15s) expires and another node wins.

**Trade-off:** A 15-second gap in scheduling on leader crash is acceptable for a job scheduler. ZooKeeper/etcd would give stronger guarantees but add significant operational overhead for a project where Redis is already a dependency. The TTL is configurable — time-sensitive systems tune it down; high-churn systems tune it up.

**Rejected alternative:** Leaderless scheduling with `SKIP LOCKED` on all nodes simultaneously. This scales better but makes audit logs harder to reason about and requires more complex coordination for exactly-once semantics. Documented as a Phase 3 upgrade path.

---

### ADR-2 — `SELECT FOR UPDATE SKIP LOCKED` for job dispatch

**Context:** The leader must claim a batch of due jobs atomically without blocking other operations.

**Decision:** Use PostgreSQL's `SELECT ... FOR UPDATE SKIP LOCKED`. This acquires row-level locks on claimed rows and skips rows already locked by another transaction — no blocking, no double-dispatch.

**Trade-off:** This is a PostgreSQL-specific feature. It couples the scheduler to PostgreSQL. This is an acceptable trade-off given PostgreSQL is already the system of record for job state and the ACID guarantees it provides are non-negotiable.

**Why not application-level locking?** Application-level locks (e.g. `synchronized` block, Redis lock before DB read) still require the DB read first, meaning two threads can read the same row before either acquires the lock. `SKIP LOCKED` delegates concurrency control to the DB's lock manager, which is already ACID-correct.

---

### ADR-3 — Three separate transactions in WorkerPoolService

**Context:** Job execution can take minutes. The naive approach wraps the entire `executeJob()` method in one `@Transactional` boundary, holding a DB connection open for the full duration.

**Decision:** Three short, focused transactions: (1) create execution record, (2) mark succeeded/update next_run_at, (3) mark failed/schedule retry. The handler's `execute()` call sits outside any transaction.

**Trade-off:** If the node crashes between step 1 and step 2, the execution record stays in RUNNING state. The stale detector (ADR-5) handles this. The alternative — one long transaction — would exhaust the Hikari connection pool (default size 10) with just 10 concurrent long-running jobs.

**Concretely:** With a 10-connection pool and 10 jobs each holding a transaction for 30 seconds, the 11th request blocks for 30 seconds. With three short transactions, each connection is held for < 50ms.

---

### ADR-4 — Exponential backoff capped at 1 hour + DLQ, not infinite retry

**Context:** Failed jobs need retry logic. Naive infinite retry with fixed delay hammers a broken downstream service and wastes worker threads.

**Decision:** Retry with `2^attempt` second backoff (2s, 4s, 8s, 16s, 32s), capped at 3600 seconds. After `maxRetries` attempts, move to the dead letter queue with the full failure context.

**Trade-off:** An operator must manually inspect and requeue DLQ jobs. This is intentional — a job that fails 3 times probably has a non-transient cause (bad payload, downstream outage, schema mismatch). Automatic infinite retry would mask these bugs.

**The cap:** Without the 1-hour cap, a job on its 20th attempt would wait 12 days. The cap keeps retries in a range where operators will see them within a reasonable window.

---

### ADR-5 — Cycle detection at registration time, not at execution time

**Context:** DAG dependency chains can contain cycles (A → B → A), which would cause WAITING jobs to wait forever.

**Decision:** Run an iterative DFS cycle check at job registration time. If a cycle is detected, return a 400 with a descriptive message and roll back the transaction — no partial state is written.

**Trade-off:** Registration becomes slightly slower for large dependency graphs. This is acceptable — job registration is infrequent compared to scheduling and execution. The alternative (detecting cycles at evaluation time) would leave orphaned WAITING jobs in the database with no automatic recovery path.

**Why iterative DFS, not recursive?** A recursive DFS on a dependency graph with depth N risks a StackOverflowError. An iterative DFS with an explicit stack is safe regardless of graph depth.

---

## Running Locally

### Single node

```bash
# Start PostgreSQL
docker run -d --name scheduler-db \
  -e POSTGRES_DB=job_scheduler \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 postgres:15

# Start Redis
docker run -d --name scheduler-redis -p 6379:6379 redis:7

# Run the application
./mvnw spring-boot:run

# Open the dashboard
open http://localhost:8080/dashboard.html
```

### Multi-node (recommended for seeing distributed behaviour)

```bash
docker-compose up --build

# Node 1 (leader candidate): http://localhost:8080
# Node 2 (follower):         http://localhost:8081

# Kill the leader and watch failover within 15 seconds
docker stop scheduler-node-1
docker logs -f scheduler-node-2
```

### Running tests

```bash
# Requires Docker running — Testcontainers pulls postgres:15 and redis:7
./mvnw test
```

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/jobs` | Register a new job |
| `GET` | `/api/v1/jobs` | List all jobs |
| `GET` | `/api/v1/jobs/{id}` | Get job details |
| `DELETE` | `/api/v1/jobs/{id}` | Delete a job |
| `PATCH` | `/api/v1/jobs/{id}/pause` | Pause a scheduled job |
| `PATCH` | `/api/v1/jobs/{id}/resume` | Resume a paused job |
| `POST` | `/api/v1/jobs/{id}/trigger` | Trigger immediately |
| `GET` | `/api/v1/jobs/{id}/executions` | Execution history |
| `GET` | `/api/v1/dlq` | List dead letter jobs |
| `POST` | `/api/v1/dlq/{id}/requeue` | Re-enqueue a dead job |
| `GET` | `/api/v1/metrics` | Scheduler health + leader info |
| `GET` | `/actuator/metrics` | Full Micrometer metrics |
| `GET` | `/actuator/health` | Application health |

### Example: register a cron job

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Daily invoice generation",
    "jobType": "EMAIL_NOTIFICATION",
    "scheduleType": "CRON",
    "cronExpr": "0 9 * * *",
    "payload": "{\"template\": \"invoice\", \"region\": \"IN\"}",
    "maxRetries": 3
  }'
```

### Example: register a DAG chain

```bash
# Step 1: register the parent
PARENT_ID=$(curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"name":"Export data","jobType":"EMAIL_NOTIFICATION",
       "scheduleType":"ONE_TIME","runAt":"2025-01-01T10:00:00Z","maxRetries":2}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

# Step 2: register the child that depends on the parent
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"Send report\",\"jobType\":\"EMAIL_NOTIFICATION\",
       \"scheduleType\":\"ONE_TIME\",\"runAt\":\"2025-01-01T10:00:00Z\",
       \"maxRetries\":2,\"dependsOn\":[\"$PARENT_ID\"]}"
```

---

## Project Structure

```
src/main/java/com/scheduler/
├── config/
│   ├── AsyncConfig.java          # Dedicated thread pool for job execution
│   ├── NodeIdentityService.java  # Stable per-JVM node ID (hostname + UUID)
│   └── RedisConfig.java          # StringRedisTemplate + Lua release script
├── controller/
│   └── JobController.java        # REST API — thin, delegates to JobService
├── domain/
│   ├── entity/                   # JPA entities (Job, JobExecution, DeadLetterJob, JobDependency)
│   └── enums/                    # JobStatus, ScheduleType, ExecutionStatus
├── dto/                          # Request/response objects — API contract
├── exception/                    # Domain exceptions + GlobalExceptionHandler
├── handler/                      # JobHandler interface, registry, sample handlers
├── repository/                   # Spring Data JPA repositories
├── scheduler/
│   └── JobSchedulerEngine.java   # @Scheduled poller — leader guard + SKIP LOCKED dispatch
└── service/
    ├── DagEvaluatorService.java       # DAG dependency evaluation + cycle detection
    ├── DistributedLockService.java    # Redis SET NX PX + Lua atomic release
    ├── JobService.java                # CRUD, lifecycle, dependency registration
    ├── LeaderElectionService.java     # Redis SETNX leader election + lease refresh
    ├── SchedulerMetricsService.java   # Micrometer counters, gauges, timers
    ├── StaleJobResetService.java      # Detects crashed-node RUNNING jobs and resets them
    └── WorkerPoolService.java         # @Async job execution, retry, DLQ, metrics
```

---

## Key Metrics

After running, these are available at `/actuator/metrics/{name}`:

| Metric | Type | Description |
|---|---|---|
| `scheduler.jobs.succeeded` | Counter | Successful executions (tagged by `jobType`) |
| `scheduler.jobs.failed` | Counter | Failed attempts (tagged by `jobType`) |
| `scheduler.jobs.dlq.added` | Counter | Jobs moved to DLQ |
| `scheduler.leader.elections.won` | Counter | Times this node won election |
| `scheduler.jobs.stale.reset` | Counter | Stale RUNNING jobs recovered |
| `scheduler.queue.depth` | Gauge | Live count of SCHEDULED jobs |
| `scheduler.jobs.running` | Gauge | Live count of RUNNING jobs |
| `scheduler.jobs.waiting` | Gauge | Live count of WAITING (DAG-blocked) jobs |
| `scheduler.job.execution.duration` | Timer | Execution duration with p50/p95/p99 (tagged by `jobType`, `outcome`) |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Framework | Spring Boot 3.2, Java 17 |
| Persistence | PostgreSQL 15, Spring Data JPA, Hibernate |
| Coordination | Redis 7 (leader election, distributed locks) |
| Observability | Micrometer, Spring Actuator |
| Testing | JUnit 5, Testcontainers, Awaitility |
| Build | Maven |
| Container | Docker, Docker Compose |
