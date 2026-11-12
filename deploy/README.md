# Self-hosted backend deployment

This directory contains the Phase 1/2 implementation of
[`Shared/MIGRATION_PLAN_SELF_HOSTED.md`](../Shared/MIGRATION_PLAN_SELF_HOSTED.md):
the existing backend now runs in Docker on a self-hosted server while the Azure
Function Apps remain deployed and unchanged.

## What this phase does and does not do

**Does**

- Runs the existing `API` and `Backend` projects in the official .NET 10 Azure
  Functions isolated-worker container image. No backend code is forked.
- Splits the Backend into separate API, scheduled, and heavy-worker containers
  using function-level selection, so heavy queue processing cannot starve HTTP
  traffic.
- Adds Caddy with automatic TLS, Docker Compose, Redis, Prometheus, Grafana,
  cAdvisor, node-exporter, and backups.
- Provides a GitHub Actions deployment workflow and repeatable host
  provisioning (bootstrap script and Ansible playbook).

**Does not do yet**

- Cosmos DB is **not** moved. It remains the source of truth.
- Azure Service Bus is **not** moved. Workers still consume the existing queues.
- PostgreSQL is **not** deployed. That is deliberately deferred to Phase 5.
- Redis is deployed and configured, but no application code uses it yet. It is
  ready for the Phase 4 caching work. Its current eviction policy is
  `allkeys-lru` because it is a cache; change that before using the same Redis
  for durable queues.

## Architecture

```
                         Internet
                            |
                    +-------v--------+
                    |     Caddy      |  80/443, automatic TLS
                    +---+--------+---+
                        |        |
             api.domain |        | backend.domain        grafana.domain
                        |        |                            |
             +----------v--+  +--v---------------+     +------v------+
             |     api     |  |   backend-api    |     |   grafana   |
             |  (all API   |  | (Backend HTTP:   |     +------+------+
             |  functions) |  |  webhook, admin) |            |
             +------+------+  +--------+---------+            |
                    |                  |                      |
                    +---------+--------+----------------------+
                              |
                       Docker network
                              |
        +---------------------+----------------------+
        |                     |                      |
 +------v------+      +-------v-------+      +-------v-------+
 |worker-      |      | worker-heavy  |      | redis /       |
 |schedule     |      | (Service Bus  |      | prometheus /  |
 |(timers)     |      | + change feed)|      | exporters     |
 +------+------+      +-------+-------+      +---------------+
        |                     |
        +----------+----------+
                   |
        Azure Service Bus + Cosmos DB + Blob Storage
        (still the production backing services)
```

The `worker-schedule` and `worker-heavy` containers are behind Compose profiles
and are **off by default**. This lets the self-hosted API be verified without
stealing Service Bus messages or double-running timer jobs from Azure.

## Migration switches

| Switch | Scope | Effect |
| --- | --- | --- |
| `COMPOSE_PROFILES` | Compose | `workers` enables heavy queue/change-feed workers; `schedules` enables timer jobs |
| `AzureFunctionsJobHost__functions__N` | Container | Selects exactly which Functions each container loads |
| `AzureWebJobs.<Function>.Disabled` | Container | Legacy per-function disable flag; mirror any disabled functions from the Azure app if needed |

The self-hosted worker containers do not automatically inherit
`AzureWebJobs.<Function>.Disabled` settings from the Azure Function App. If a
function is intentionally disabled in Azure, either remove it from the relevant
`AzureFunctionsJobHost__functions__N` list or set the matching `Disabled`
variable in `.env`.

## Container / function split

| Container | Project | Functions |
| --- | --- | --- |
| `api` | `API` | All API functions (route prefix `/api`) |
| `backend-api` | `Backend` | `QueueAllScrapeJobsHttp`, `QueueAllVisitedPathsJobs`, `QueueAllSummitJobs`, `QueueAllVisitedAreasJobs`, `BackfillVisitedPathOsmHighwayIds`, `BackfillVisitedPathOsmHighwayIdsAll`, `BackfillAdminBoundaryMetrics`, `TestScrapeUrl`, `RunSummitsForActivity`, `RefreshStravaToken`, `StravaWebhook`, `ScrapeRaceHttp`, `StoreCuratedPeaksGroups`, `CleanupExcludedActivityTypes` |
| `worker-schedule` | `Backend` | All `Discover*` timer functions plus `QueueAllScrapeJobs` |
| `worker-heavy` | `Backend` | All Service Bus and Cosmos change-feed workers |

`scripts/verify-function-coverage.sh` fails CI if a Backend function is added
without being assigned to exactly one container.

## First-time setup

### 1. DNS

Create A/AAAA records for the three hostnames to the server:

- `API_DOMAIN` (for example `api.example.com`)
- `BACKEND_DOMAIN` (for example `backend.example.com`)
- `GRAFANA_DOMAIN` (for example `grafana.example.com`)

Ports 80 and 443 must be reachable from the internet for Caddy to obtain
certificates.

### 1b. Network access to Azure

The server must be able to reach the existing Cosmos DB account, Service Bus
namespace, Storage account, and SignalR service, plus Strava, Mistral, and
Overpass. If those Azure resources use IP firewalls or private endpoints, allow
the server's egress IP. `CosmosConnectionMode=Gateway` is the default because it
tolerates container networking better than Direct mode.

### 2. Provision the server

On a fresh Ubuntu or Debian host:

```bash
git clone <this repository>
cd strava
sudo ./deploy/scripts/bootstrap-server.sh
```

The script installs Docker, the Compose plugin, firewall rules, unattended
upgrades, fail2ban, log rotation, copies the deployment files to
`/opt/stacks/strava`, generates `.env` from the example, and installs the
daily backup timer.

Ansible users can do the same with:

```bash
cd deploy/ansible
ansible-galaxy collection install -r requirements.yml
cp inventory.example.ini inventory.ini   # edit it
ansible-playbook -i inventory.ini playbook.yml
```

The Ansible playbook uploads `deploy/.env`, so create it on the controller first.

### 3. Fill in `.env`

Edit `/opt/stacks/strava/.env` (or `deploy/.env` before Ansible) and replace all
`REPLACE` values:

- `IMAGE_NAMESPACE`: lowercase GitHub owner that publishes the GHCR packages.
- `API_DOMAIN`, `BACKEND_DOMAIN`, `GRAFANA_DOMAIN`, `ACME_EMAIL`.
- `CosmosDBConnection`, `ServiceBusConnection`, `AzureWebJobsStorage`,
  `BlobStorageConnection`, `AzureSignalRConnectionString`.
- `StravaClientSecret`, `stravaVerifyToken`, `AdminApiKey`, Mistral settings.
- Generated Redis/Grafana passwords (the bootstrap script fills these in; do not
  reuse the example values).

`CosmosAccountResourceId` can stay empty. Live RU/s reporting then falls back to
“unavailable” because the host has no Azure identity. Set it only if you also
provide Azure credentials to the containers.

Keep `COMPOSE_PROFILES=` empty for the first deployment.

### 4. Allow the server to pull private images

If the GHCR packages are private, log in once on the server with a read-only
personal access token:

```bash
docker login ghcr.io -u <github-user>
```

Alternatively make the two packages public in GitHub package settings.

### 5. Start the stack

```bash
cd /opt/stacks/strava
docker compose up -d --wait
docker compose ps
```

At this point:

- `https://API_DOMAIN/api/...` serves the self-hosted API.
- `https://BACKEND_DOMAIN/...` serves the Backend HTTP functions.
- `https://GRAFANA_DOMAIN` serves Grafana.
- No workers are consuming Azure Service Bus queues or running timers.

Run the public smoke test when DNS/TLS are ready:

```bash
./scripts/smoke-test.sh
```

### Local build (optional)

The production compose file only pulls images. To build and run the containers
locally while still using the Azure backing services:

```bash
cd deploy
cp .env.example .env
# set API_DOMAIN=api.localhost, BACKEND_DOMAIN=backend.localhost,
# GRAFANA_DOMAIN=grafana.localhost, then fill in the Azure values
docker compose -f docker-compose.yml -f docker-compose.local.yml \
  --profile workers --profile schedules up -d --build
```

Caddy uses its internal CA for `*.localhost`.

## GitHub Actions automatic deploy

`.github/workflows/selfhosted-deploy.yml` builds both images, pushes them to
GHCR tagged with the commit SHA and `latest`, syncs `deploy/` to the server
over SSH (without touching `.env` or backups), validates Caddy/Prometheus
configuration, then pulls and restarts the stack.

Configure a GitHub Environment named `self-hosted` with these secrets:

| Secret | Meaning |
| --- | --- |
| `SELFHOSTED_HOST` | Server hostname or IP |
| `SELFHOSTED_USER` | SSH user with access to Docker; use the same user passed as `DEPLOY_USER` during bootstrap |
| `SELFHOSTED_PORT` | SSH port (optional, defaults to 22) |
| `SELFHOSTED_SSH_KEY` | Private SSH key for that user |

The deployment script (`/opt/stacks/strava/scripts/deploy.sh`) pulls the new
image tag, writes it to `.env`, waits for health checks, and automatically rolls
back to the previous tag if startup fails.

Changes to the systemd backup units under `deploy/systemd/` are copied by the
sync but still need `sudo cp … /etc/systemd/system/ && sudo systemctl daemon-reload`
or a bootstrap re-run to take effect.

Manual rollback:

```bash
/opt/stacks/strava/scripts/deploy.sh <previous-commit-sha>
```

Manual deploy of an existing tag:

```bash
cd /opt/stacks/strava
./scripts/deploy.sh <tag>
```

The official Functions runtime image is currently `linux/amd64` only, so the
workflow builds amd64 images and the Compose services are pinned to
`linux/amd64`. On an ARM server Docker will run them through emulation.

## Rollout order

### Step 1 - verify the self-hosted API in parallel

Leave `COMPOSE_PROFILES=` empty. Test the self-hosted API directly via
`API_DOMAIN`, or point a staging client at it. Cosmos and Service Bus are shared
with Azure, and no queues are consumed by the new stack.

When you are ready to send real traffic to the self-hosted API, update the
frontend/mobile API base URL and the Strava webhook callback URL to the new
`API_DOMAIN` / `BACKEND_DOMAIN`. Until then, both deployments remain live.

### Step 2 - move the heavy workers

First decide how you want to split Service Bus traffic:

- **Competing consumers (recommended for validation):** leave the Azure Backend
  app running and set `COMPOSE_PROFILES=workers`. Both the Azure functions and
  `worker-heavy` consume from the same queues; Service Bus delivers each message
  to only one of them. This validates throughput without a hard cutover.
- **Self-hosted only:** disable the corresponding worker functions in the Azure
  Backend app (or stop the app), then set `COMPOSE_PROFILES=workers`.

```bash
# edit /opt/stacks/strava/.env
COMPOSE_PROFILES=workers

cd /opt/peakshunters
docker compose up -d --wait
```

The Cosmos change-feed triggers (`QueueActivityJobs`,
`OsmFeaturesChangeTrigger`, `NewSummitedPeak`) use the same lease containers as
Azure. They coordinate through leases, so leave both running only if you are
comfortable with lease hand-off during container restarts.

### Step 3 - move the scheduled jobs

Timer functions are not competing consumers: if both Azure and the self-hosted
worker run, each schedule fires twice. Disable the matching timer functions in
Azure **before** enabling the profile:

```bash
COMPOSE_PROFILES=workers,schedules
```

### Step 4 - later phases

- Phase 4: wire Redis caching and queue/backpressure changes into `Shared`.
- Phase 5: add Postgres only when read offloading is ready to be implemented.
- Phase 6+: dual-write and final Cosmos cutover.

## Configuration notes

- Connection strings and secrets are read from `/opt/stacks/strava/.env` via
  Compose `env_file`. They are never baked into images.
- `AzureFunctionsJobHost__functions__N` selects which functions each container
  loads. The `api` container intentionally loads all API functions.
- `AzureFunctionsWebHost__hostid` is unique per container so the Functions host
  does not collide on timer/listener locks when sharing the Azure storage
  account with the Azure Function Apps.
- The Functions host administration API (`/admin/*`) is blocked at Caddy. Use an
  SSH tunnel if you need host-level diagnostics.
- `AuthorizationLevel.Function` endpoints still require a Functions key. By
  default the key store lives in the configured `AzureWebJobsStorage` account
  under the self-hosted host ID. Retrieve or manage keys through the Functions
  admin API over an SSH tunnel, or switch `AzureWebJobsSecretStorageType` to
  `files` and persist the key directory if you prefer local key storage. Caddy
  forwards the `x-functions-key` header unchanged.

## Monitoring

- Prometheus: bound to `127.0.0.1:9090` on the host (use an SSH tunnel).
- Grafana: `https://GRAFANA_DOMAIN`, provisioned with the Prometheus datasource
  and the “Peakshunters self-hosted” dashboard.
- Alerts: target down, HTTP probe failure, host CPU/memory/disk, container
  restarts, and Redis memory pressure. Prometheus evaluates the rules; add an
  Alertmanager/contact point if you want out-of-band notifications.
- Logs: `docker compose logs -f --tail=200`, with json-file rotation at
  20 MB x 5 per container.

Azure-side metrics (Cosmos RU usage, Function executions) are not scraped yet.
Use the Azure portal or Azure Monitor while the migration is in the parallel
phase; an Azure Monitor exporter can be added later.

## Backups

`scripts/backup.sh` backs up:

- `.env` (mode 600)
- rendered Compose state with secrets redacted
- Redis RDB snapshot
- Grafana and Caddy volumes
- optional Prometheus data (`BACKUP_PROMETHEUS=1`)

The systemd timer runs daily at 03:30 UTC and keeps 14 days by default
(`BACKUP_RETENTION_DAYS`). Cosmos DB and Azure Storage are still platform-backed
and are not part of this script.

Restore into a stopped stack:

```bash
docker compose down
./scripts/restore.sh /opt/stacks/strava/backups/<timestamp>
```

## Security checklist

- SSH key authentication only; disable password login in `sshd_config`.
- UFW allows only the SSH port, 80/tcp, 443/tcp, and 443/udp.
- Only Caddy publishes ports. Redis, Grafana, Prometheus, and the Functions
  containers stay on the internal Docker network (Prometheus is bound to
  localhost only).
- `/admin/*` is not publicly proxied.
- Keep `.env` at mode 600 and out of git.
- Protect the `self-hosted` GitHub Environment with required reviewers if you
  want manual approval before production deploys.
