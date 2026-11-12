# Self-Hosted Migration Plan

## Goal

Reduce Azure costs by moving database-heavy and compute-heavy backend workloads onto a self-hosted, containerized server while keeping the project operational during the transition.

Primary pain points today:

- Cosmos DB free-tier bottlenecks
- Azure Functions scaling too aggressively
- Excessive database reads and writes

This plan is phased so parts can move independently without requiring a full shutdown.

---

## Implementation status in this repository

The first self-hosted slice is implemented under [`deploy/`](../deploy/README.md).
See [`deploy/DEPLOY.md`](../deploy/DEPLOY.md) for step-by-step deployment
instructions.

### What is done

- **Docker Compose stack** with `api`, `backend-api`, `worker-schedule`
  (profile: `schedules`), and `worker-heavy` (profile: `workers`) containers.
  The existing `API` and `Backend` Azure Functions projects run unchanged in the
  official .NET 10 isolated-worker image — no backend code is forked.
- **Internal-only deployment.** No public ports, no TLS, no reverse proxy.
  The server lives on the home network and connects outbound to Azure. The Azure
  API Function App stays up and handles all external traffic during this phase.
- **Stack layout** follows the server's Dockge conventions:
  - Compose stack at `/opt/stacks/strava`
  - Persistent data at `/opt/data/strava`
- **GitHub Actions workflow** (`.github/workflows/selfhosted-deploy.yml`)
  builds both images, pushes them to GHCR tagged with the commit SHA, rsyncs
  `deploy/` to the server over SSH, and runs `scripts/deploy.sh` which pulls,
  starts, health-checks, and auto-rolls-back on failure.
- **Existing Azure workflows** are unchanged and kept running in parallel.
  They ignore `deploy/**` and `Shared/MIGRATION_PLAN_SELF_HOSTED.md` so
  infra-only commits do not trigger Azure deploys.
- **Function coverage gate** — `scripts/verify-function-coverage.sh` fails CI
  if a Backend function is added without being assigned to exactly one container.
- **Monitoring** via the existing Beszel instance on the server.
- **Automated daily backups** of `.env` via systemd timer (03:30 UTC, 14-day
  retention). Cosmos DB and Azure Storage have their own platform backups.
- **Bootstrap** — `scripts/bootstrap-server.sh` provisions a fresh Ubuntu/Debian
  host (Docker, UFW, fail2ban, unattended upgrades). Ansible alternative in
  `ansible/playbook.yml`.

### What is not done yet

- Workers are off by default (`COMPOSE_PROFILES=` empty). Not yet validated
  against production traffic — this is the immediate next step.
- Cosmos DB, Azure Service Bus, Azure Blob Storage remain on Azure.
- Redis and caching (Phase 4) — deferred until workers are stable.
- PostgreSQL (Phase 5) — not deployed.
- No DNS, no public access to the self-hosted API yet.

### Decisions made during implementation

- **No Caddy/TLS** — not needed for an internal worker-only deployment.
- **No Prometheus/Grafana** — Beszel already covers host and container metrics.
- **No Redis yet** — no application code uses it; will be added when Phase 4
  caching work begins. Note: use `allkeys-lru` eviction for cache-only use,
  but change it before using Redis for durable queues.
- **Cosmos change-feed triggers** (`QueueActivityJobs`, `OsmFeaturesChangeTrigger`,
  `NewSummitedPeak`) share lease containers with Azure. Running both simultaneously
  causes lease hand-off on container restarts — disable Azure-side before enabling
  `worker-heavy` for 100% self-hosted.

## Recommended migration approach

### Short version

1. Move the backend workloads that create the most DB pressure first
2. Keep Cosmos DB temporarily, but reduce load through caching, batching, and queue-based processing
3. Containerize all new backend services
4. Add a local database later for read offloading and eventual full migration
5. Cut over the primary database only after the backend is stable on the new server

---

## Target architecture

Initial self-hosted stack:

- Reverse proxy: Caddy or Nginx
- API/backend container
- Worker containers for heavy/background jobs
- Redis for caching and queueing
- PostgreSQL for eventual migration target and/or read replicas/materialized data
- Monitoring: Prometheus + Grafana
- Automated backups

This allows a hybrid state where:

- Cosmos DB is still active
- Some jobs run self-hosted
- Some reads come from cache or local Postgres
- The system remains usable during the migration

---

## Phase 0 - Inventory and baseline

### 0.1 Document current Azure usage

List all:

- Cosmos DB containers/collections
- Azure Functions
- Function triggers (HTTP, timer, queue, event)
- App endpoints that hit Cosmos heavily
- Scheduled jobs and background processes
- Blob/file storage if any
- Monthly cost by service

### 0.2 Identify high-load patterns

For each backend feature, identify whether the DB pressure comes from:

- repeated reads
- polling
- duplicate job execution
- many small writes
- expensive derived queries
- retry storms or overlapping function runs

### 0.3 Prioritize workloads

Classify work into:

- **Move first:** heavy jobs, batch tasks, scheduled tasks, expensive read endpoints
- **Move second:** normal APIs with predictable access patterns
- **Move last:** primary write-critical DB flows and anything tightly coupled to Cosmos

---

## Phase 1 - Build the self-hosted base

### 1.1 Provision the server

Requirements:

- Linux host
- SSD/NVMe storage
- enough RAM for API, workers, Redis, Postgres, and other co-hosted services
- static IP or stable DNS

### 1.2 Install platform components

Set up:

- Docker
- Docker Compose
- restart policies
- firewall
- SSH hardening
- automatic security updates

### 1.3 Prepare data and service directories

Use persistent volumes or directories for:

- PostgreSQL data
- Redis data if needed
- logs
- backups
- app-specific persistent state

### 1.4 Add networking and TLS

Set up:

- reverse proxy
- TLS certificates
- internal Docker networking
- private-only DB and Redis access

### 1.5 Add monitoring first

Before migration, install:

- Prometheus
- Grafana
- container monitoring
- host CPU/memory/disk alerts
- uptime checks

---

## Phase 2 - Containerize backend without changing the database yet

### 2.1 Refactor Azure Functions into app services and workers

Convert:

- HTTP functions -> API container
- timer functions -> scheduled worker container
- queue/event functions -> worker consumers

Important design rule:

- Keep user-facing API separate from heavy workers

### 2.2 Externalize configuration

Move all config to environment variables:

- Cosmos connection settings
- Redis settings
- future Postgres settings
- API keys and secrets
- feature flags

### 2.3 Create a Compose stack

Suggested services:

- `proxy`
- `api`
- `worker-default`
- `worker-heavy`
- `redis`
- `postgres`
- `prometheus`
- `grafana`

### 2.4 Add migration feature flags

Introduce toggles for:

- Azure Function vs self-hosted worker
- direct Cosmos read vs cache/local DB read
- dual-write behavior later
- per-endpoint or per-job routing

These flags are essential for partial cutovers and easy rollback.

---

## Phase 3 - Move database-heavy backend workloads first

This is the best first live migration step if the goal is to reduce Azure cost and lower Cosmos load without migrating the primary database yet.

### 3.1 Move heavy jobs to self-hosted workers

Take the most DB-intensive Azure Functions and move them into worker containers.

Add:

- bounded concurrency
- retries with backoff
- idempotency controls
- structured logging

### 3.2 Introduce Redis queueing

Replace direct or highly parallel execution with:

- job queue
- worker consumers
- explicit concurrency limits

This prevents bursty scale-out and duplicate work.

### 3.3 Cut over one job type at a time

For each workload:

1. deploy the self-hosted version
2. test manually or in shadow mode
3. switch only that job type
4. monitor DB reads/writes and runtime
5. keep Azure fallback available

---

## Phase 4 - Reduce Cosmos DB load before full DB migration

Moving backend alone is not enough unless access patterns also change.

### 4.1 Add Redis caching for repeated reads

Use cache for:

- frequently requested records
- status endpoints
- settings/config
- expensive derived responses
- dashboards and summaries

Pattern:

- check Redis first
- query Cosmos on cache miss
- cache with TTL

### 4.2 Precompute expensive read models

For repeated derived queries:

- run a background worker
- build simplified result documents/views
- store in Redis or local Postgres
- serve those instead of recomputing against Cosmos each time

### 4.3 Reduce polling

Identify endpoints or jobs that repeatedly poll Cosmos.

Replace with:

- cache-backed status tracking
- slower poll intervals
- push/notification model if practical
- storing transient state outside Cosmos where possible

### 4.4 Batch and debounce writes

Instead of many tiny writes:

- queue write intents
- merge updates where possible
- flush in batches
- avoid writing intermediate states unless necessary

### 4.5 Prevent duplicate processing

Use:

- locks
- deduplication keys
- controlled worker concurrency
- idempotent job execution

This can reduce unnecessary Cosmos reads/writes significantly.

---

## Phase 5 - Add local Postgres for read offloading

This phase reduces Cosmos pressure further without fully replacing it yet.

### 5.1 Deploy Postgres

Set up PostgreSQL in Docker with:

- persistent storage
- daily backups
- tested restore process

### 5.2 Mirror the hottest read data

Copy selected Cosmos entities into Postgres using:

- scheduled sync jobs
- incremental updates where feasible

Use Postgres first for:

- history views
- reporting
- summaries
- expensive read-heavy endpoints

### 5.3 Keep writes in Cosmos initially

At this stage:

- Cosmos remains source of truth for writes
- Postgres is used to offload reads
- the application stays operational in hybrid mode

This is a good intermediate state if the biggest issue is read pressure.

---

## Phase 6 - Prepare full database migration

Once the backend is stable and Cosmos load is reduced, prepare the final move away from Cosmos.

### 6.1 Design the target schema

Map Cosmos data into PostgreSQL using:

- normal tables where appropriate
- `jsonb` for semi-structured data
- indexes on known query paths

### 6.2 Build migration tooling

Create scripts for:

- initial bulk export from Cosmos
- transformation into Postgres format
- import into Postgres
- validation reports and record counts

### 6.3 Add selective dual-write

For critical entities:

- write to Cosmos and Postgres
- verify consistency
- switch reads to Postgres later

---

## Phase 7 - Final database cutover

Only do this after the self-hosted backend is working well and restore procedures are tested.

### 7.1 Schedule a short maintenance window

Because uptime is not critical, use a small controlled write freeze.

During the window:

- pause writes or queue them
- run final sync from Cosmos
- validate counts and key records
- switch primary DB connections to Postgres
- resume normal traffic

### 7.2 Keep Cosmos as temporary fallback

After cutover:

- keep Cosmos available read-only for a period
- retain exports and recovery scripts
- do not decommission immediately

---

## Phase 8 - Decommission Azure gradually

Turn off Azure services only after confidence is high.

Recommended order:

1. migrated Azure Functions
2. Azure-side queue/event plumbing no longer in use
3. Cosmos DB after validation period
4. any remaining support services

---

## Rollback strategy

### For backend/worker cutovers

Rollback by:

- routing the workload back to Azure Functions
- pausing local workers

### For read offloading

Rollback by:

- switching read paths from Postgres/cache back to Cosmos

### For final DB migration

Rollback requires:

- Cosmos still available
- recent verified backups
- feature flags or config to switch DB target

---

## Recommended execution order

1. Inventory Azure usage and identify DB-heavy workloads
2. Provision self-hosted server and observability
3. Containerize backend services
4. Move the heaviest Azure Functions/jobs first
5. Add Redis caching and queues
6. Reduce polling and batch writes
7. Add Postgres for read offloading
8. Migrate reads from Cosmos where possible
9. Prepare dual-write and full data migration
10. Perform final DB cutover
11. Decommission Azure services in stages

---

## Immediate next actions

### Action 1 - Identify top DB pressure sources

List:

- the top 5 Cosmos-heavy endpoints/jobs
- read frequency
- write frequency
- whether each one can be cached, batched, or precomputed

### Action 2 - Pick the first workload to move

Choose the first backend process to self-host based on:

- highest Cosmos usage
- lowest migration risk
- easy rollback

### Action 3 - Define the first self-hosted container stack

Initial stack:

- API
- heavy worker
- Redis
- reverse proxy
- monitoring

### Action 4 - Measure success

Track:

- Cosmos request volume
- Cosmos RU usage or equivalent usage metrics
- Azure Function executions
- backend response times
- worker queue depth

---

## Core conclusion

Yes, the backend can move first before the database, but it will only reduce DB load if the migration also changes how the backend interacts with Cosmos.

The best low-risk path is:

- move DB-heavy backend jobs first
- add caching and queue-based processing
- offload heavy reads to local infrastructure
- migrate the primary database later

That sequence should give earlier cost savings and lower operational risk than trying to move everything at once.
