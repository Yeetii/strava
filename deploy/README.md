# Self-hosted backend deployment

This directory contains the Phase 1/2 implementation of
[`Shared/MIGRATION_PLAN_SELF_HOSTED.md`](../Shared/MIGRATION_PLAN_SELF_HOSTED.md):
the existing backend now runs in Docker on a self-hosted server while the Azure
Function Apps remain deployed and unchanged. The stack is internal-only: no
ports are published and it is reachable only from the home network, over an
SSH tunnel, or through a Tailscale tailnet (which is also how GitHub Actions
deploys reach it).

## What this phase does and does not do

**Does**

- Runs the existing `API` and `Backend` projects in the official .NET 10 Azure
  Functions isolated-worker container image. No backend code is forked.
- Splits the Backend into separate API, scheduled, and heavy-worker containers
  using function-level selection, so heavy queue processing cannot starve HTTP
  traffic.
- Deploys them with Docker Compose as an internal-only stack: no ports are
  published, everything stays on a private Docker network.
- Runs a dedicated `pmtiles` container with tippecanoe compiled in. It builds
  the weekly race-tile archive from organizer data on a shared host directory;
  the API serves the result at `/tiles/trails.pmtiles`. No Azure config needed.
- Provides a GitHub Actions deployment workflow and repeatable host
  provisioning (bootstrap script and Ansible playbook).

**Does not do yet**

- Cosmos DB is **not** moved. It remains the source of truth.
- Azure Service Bus is **not** moved. Workers still consume the existing queues.
- PostgreSQL is **not** deployed. That is deliberately deferred to Phase 5.
- Redis is **not** deployed. It is deferred until the Phase 4 caching work
  actually needs it.
- There are no automated host backups yet (the backup script was removed for
  now). Cosmos DB and Azure Storage still have their own platform backups.

## Architecture

```
          Home network / Tailscale tailnet
                     |
              +------v-------+
              | Docker network|
              |   (backend)   |
              +--+---------+--+
                 |         |
        +--------v----------------+
        |           api          |
        | (API + Backend HTTP:   |
        |  endpoints, webhook,   |
        |  token refresh, admin) |
        +------------------------+

  worker-schedule   timer jobs - profile "schedules" (off by default)
  worker-heavy      Service Bus + Cosmos change feed - profile "workers"
                    (off by default)
  pmtiles           weekly race-tile build; shares the host race-data directory with api
                     |
                     v
      Azure Service Bus + Cosmos DB + Blob Storage
      (still the production backing services)
```

The `worker-schedule` and `worker-heavy` containers are behind Compose profiles
and are **off by default**. This lets the self-hosted API be verified without
stealing Service Bus messages or double-running timer jobs from Azure.

## Migration switches

| Switch | Scope | Effect |
| --- | --- | --- |
| `COMPOSE_PROFILES` | Compose | `workers` enables queue/change-feed workers; `schedules` enables timer jobs |
| `WORKER_ROLE` | Container | Selects the plain-host role: `api`, `schedule` or `heavy` |
| `REDIS_CONNECTION_STRING` | Container | Hangfire storage (durable Redis: AOF + `noeviction`) |

Trigger discovery is automatic: a queue message becomes a Hangfire job executed
by the `heavy` role, a Cosmos trigger becomes an in-process change-feed
processor, and a timer becomes a Hangfire recurring job in the `schedule` role.
No per-function whitelist exists any more.

## Container / role split

| Container | Project | Role | Work |
| --- | --- | --- | --- |
| `api` | `API` + `Backend` | `api` | All API endpoints plus Backend HTTP functions (webhook, token refresh, admin, backfill) on Kestrel |
| `worker-schedule` | `Backend` | `schedule` | 12 Hangfire recurring jobs |
| `worker-heavy` | `Backend` | `heavy` | 12 queue consumers + 3 Cosmos change-feed processors |
| `redis` | — | — | Durable Hangfire storage |
| `pmtiles` | `PmtilesJob` | — | Weekly race-tile build (Hangfire scheduler) |
| `api` (also) | — | — | Serves `/tiles/trails.pmtiles` from the shared host directory |

`scripts/verify-role-coverage.sh` fails CI if the trigger inventory no longer
matches the plan (12 queues / 3 feeds / 12 crons) or if legacy function-list
env vars reappear.

### Race tiles

The `pmtiles` image has tippecanoe compiled in and needs **no Azure connection
strings**: organizer JSON and route GPX are read from the host directory
`RACE_DATA_DIR` (`/opt/data/strava/races`, mounted at `/data/races`), and the built archive is written to
`/data/races/tiles/trails.pmtiles`. The `api` container mounts the same directory
(writable, for admin organizer updates) and serves the archive publicly with
HTTP range support. `worker-schedule` and `worker-heavy` mount it too and also
use the FileSystem store, so discoveries, scrapes and the weekly build all see
the same data.

```bash
# force a one-shot build (the container's default is the weekly scheduler)
docker compose run --rm pmtiles build-race-tiles-from-organizers
```

One-time rollout seeds the host directory from Blob; see the seeding steps in [DEPLOY.md](DEPLOY.md).
Tune the container with `PMTILES_MEMORY_LIMIT` and `PMTILES_CPUS` in `.env`.

## First-time setup

### 1. Network access to Azure

The stack is internal-only: no DNS records, domains, or open inbound ports are
needed. The server must be able to reach the existing Cosmos DB account, Service Bus
namespace, Storage account, and SignalR service, plus Strava, Mistral, and
Overpass. If those Azure resources use IP firewalls or private endpoints, allow
the server's egress IP. `CosmosConnectionMode=Gateway` is the default because it
tolerates container networking better than Direct mode.

### 2. Provision the server

On a Docker host (Engine + Compose plugin + rsync already installed), with the
repository cloned to `/opt/repos/strava`:

```bash
sudo mkdir -p /opt/repos && sudo chown "$USER" /opt/repos
git clone <this repository> /opt/repos/strava
cd /opt/repos/strava
sudo ./deploy/scripts/bootstrap-server.sh
```

The bootstrap copies the deployment files from `deploy/` (Docker Compose and
scripts) to `/opt/stacks/strava` and generates `.env` from the example. The
repository stays in `/opt/repos/strava`;
`/opt/stacks/strava` is the stack's runtime directory that GitHub Actions
keeps in sync on every deploy.

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
- `CosmosDBConnection`, `AzureWebJobsStorage`,
  `BlobStorageConnection`, `AzureSignalRConnectionString`.
- `StravaClientSecret`, `stravaVerifyToken`, Mistral settings.
- `AdminApiKey`: generated by the bootstrap script; change it if desired.

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

- `api` serves all HTTP functions on the internal Docker network, reachable
  from the home network or through an SSH tunnel.
- No workers are consuming Azure Service Bus queues or running timers.

Run the smoke test:

```bash
./scripts/smoke-test.sh
```

### Local build (optional)

The production compose file only pulls images. To build and run the containers
locally while still using the Azure backing services:

```bash
cd deploy
cp .env.example .env
# then fill in the Azure values
docker compose -f docker-compose.yml -f docker-compose.local.yml \
  --profile workers --profile schedules up -d --build
```

## GitHub Actions automatic deploy

`.github/workflows/selfhosted-deploy.yml` verifies every Backend function is
assigned to a container, builds the `api`, `backend` and `pmtiles` images,
pushes them to GHCR tagged with the commit SHA and `latest`, connects the runner
to the tailnet as an ephemeral `tag:ci` node, syncs `deploy/` to the server over
Tailscale SSH
(without touching `.env`), then pulls and restarts the stack.

Tailscale SSH authenticates the deploy connections, so no SSH keys are stored
in GitHub. The server must be enrolled as a tagged node with Tailscale SSH
enabled, and the tailnet policy must allow `tag:ci → tag:server` — see
[DEPLOY.md](DEPLOY.md) for the full Tailscale setup.

Configure a GitHub Environment named `self-hosted` with these secrets:

| Secret | Meaning |
| --- | --- |
| `SELFHOSTED_HOST` | Server's Tailscale IP (100.x.y.z) or MagicDNS name |
| `SELFHOSTED_USER` | SSH user with access to Docker; use the same user passed as `DEPLOY_USER` during bootstrap, and list it in the tailnet `ssh` rule `users` |
| `TS_OAUTH_CLIENT_ID` | Tailscale OAuth client ID (writable `auth_keys` scope, tag `tag:ci`) |
| `TS_OAUTH_SECRET` | Tailscale OAuth client secret |

The deployment script (`/opt/stacks/strava/scripts/deploy.sh`) pulls the new
image tag, writes it to `.env`, waits for health checks, and automatically rolls
back to the previous tag if startup fails.

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

Leave `COMPOSE_PROFILES=` empty. Test the self-hosted API from the home
network or through an SSH tunnel. Cosmos and Service Bus are shared
with Azure, and no queues are consumed by the new stack.

The Azure Function Apps keep serving all external traffic (including the
Strava webhook) until you deliberately move clients over.

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

cd /opt/stacks/strava
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

- Phase 4: add Redis and wire caching and queue/backpressure changes into
  `Shared`.
- Phase 5: add Postgres only when read offloading is ready to be implemented.
- Phase 6+: dual-write and final Cosmos cutover.

## Configuration notes

- Connection strings and secrets are read from `/opt/stacks/strava/.env` via
  Compose `env_file`. They are never baked into images.
- `WORKER_ROLE` selects which plain-host role each container runs
  loads. The `api` container intentionally loads all API functions.
- `AzureFunctionsWebHost__hostid` is unique per container so the Functions host
  does not collide on timer/listener locks when sharing the Azure storage
  account with the Azure Function Apps.
- The stack is internal-only. Use an SSH tunnel if you need host-level
  diagnostics or the Functions administration API (`/admin/*`).
- `AuthorizationLevel.Function` endpoints still require a Functions key. By
  default the key store lives in the configured `AzureWebJobsStorage` account
  under the self-hosted host ID. Retrieve or manage keys through the Functions
  admin API over an SSH tunnel, or switch `AzureWebJobsSecretStorageType` to
  `files` and persist the key directory if you prefer local key storage.

## Monitoring

- Host and container metrics are covered by Beszel (deployed separately on
  the server).
- Logs: `docker compose logs -f --tail=200`, with json-file rotation at
  20 MB x 5 per container.

Azure-side metrics (Cosmos RU usage, Function executions) are not collected.
Use the Azure portal or Azure Monitor while the migration is in the parallel
phase.

## Security checklist

- SSH key authentication only; disable password login in `sshd_config`.
- No ports need to be reachable from outside the home network: GitHub Actions
  deploys go through the Tailscale tailnet from an ephemeral `tag:ci` node,
  authenticated by Tailscale SSH rather than SSH keys.
- No container publishes host ports; everything stays on the internal Docker
  network.
- `/admin/*` is reachable only from the internal network (use an SSH tunnel).
- Keep `.env` at mode 600 and out of git.
- Protect the `self-hosted` GitHub Environment with required reviewers if you
  want manual approval before production deploys.
