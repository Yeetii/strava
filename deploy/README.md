# Self-hosted backend deployment

This directory contains the self-hosted implementation of the migration. The
canonical strategy is the OpenChamber plan `1794838390741-self-hosted-migration-plan.md`;
the in-repo summary is [`Shared/MIGRATION_PLAN_SELF_HOSTED.md`](../Shared/MIGRATION_PLAN_SELF_HOSTED.md).
The Functions/Service Bus exit is specified in detail in
[`specs/functions-runtime-exit.md`](../specs/functions-runtime-exit.md).
Step-by-step operator instructions are in [DEPLOY.md](DEPLOY.md).

## What this phase does and does not do

**Does**

- Runs `API` and `Backend` as **plain .NET 10 hosts** — ASP.NET Core (Kestrel)
  for HTTP, Generic Hosts for workers. The Azure Functions runtime and
  `host.json` are gone; there is no forked backend codebase.
- Serves all API and Backend HTTP endpoints from a single `api` container;
  worker roles are split by `WORKER_ROLE` (`schedule`, `heavy`), not by
  per-function whitelists.
- Runs 12 queues and 12 weekly crons as **Hangfire jobs on Redis**, plus 3
  Cosmos change feeds in-process via the SDK `ChangeFeedProcessor`. Service Bus
  is no longer used.
- Uses **durable Redis** (AOF + `noeviction`) as Hangfire storage.
- Builds race tiles in a dedicated `pmtiles` container from organizer data on a
  shared host directory; the API serves `/tiles/trails.pmtiles`. No Azure config
  is needed for that container.
- Exposes the API publicly through **Tailscale Funnel** over a `*.ts.net` URL —
  no port-forwarding, no DNS record, no Caddy/TLS stack we maintain.
- Provides a GitHub Actions deploy workflow and repeatable host provisioning
  (bootstrap script and Ansible playbook), plus a role-coverage CI gate.

**Does not do yet**

- Cosmos DB, Blob Storage and Azure SignalR remain on Azure.
- PostgreSQL is not deployed — that is Phase 5 of the migration plan.
- Redis is Hangfire storage and the shared geocoding cache; **general read caching
  is not wired yet** (only geocoding uses `RedisGeocodingCache`, in both `Backend`
  and `PmtilesJob`).
- There is **no automated host backup job in this repo** (`backups/` is
  gitignored for a future one). Cosmos DB and Azure Storage keep their platform
  backups. Adding a host backup job belongs with the Postgres work.

## Architecture

```
     internet
        │
  Tailscale Funnel (TLS terminates on our node)
        │  https://<machine>.<tailnet>.ts.net
        ▼
  ┌──────────────────────── Docker network (backend) ────────────────────────┐
  │  api          Kestrel: all API + Backend HTTP routes, SignalR negotiate, │
  │               /tiles/trails.pmtiles   (publishes 127.0.0.1:8080:80)      │
  │  redis        durable Hangfire storage (AOF, noeviction)                 │
  │                                                                          │
  │  worker-schedule  12 Hangfire recurring jobs   — profile "schedules"     │
  │  worker-heavy     Hangfire queue servers + 3 change feeds — "workers"    │
  │  pmtiles          weekly race-tile build (Hangfire scheduler)            │
  └──────────────────────────────────────────────────────────────────────────┘
        │ outbound only
        ▼
  Cosmos DB · Blob Storage · Azure SignalR
  (production backing services; Service Bus and Functions are gone)
```

`worker-schedule` and `worker-heavy` are behind Compose profiles and **off by
default**, so the API can be verified without double-running timers or
stealing work from Azure.

## Migration switches

| Switch | Scope | Effect |
| --- | --- | --- |
| `COMPOSE_PROFILES` | Compose | `workers` enables queue/change-feed roles; `schedules` enables recurring jobs |
| `WORKER_ROLE` | Container | Selects the plain-host role: `api`, `schedule` or `heavy` |
| `REDIS_CONNECTION_STRING` | Container | Durable Hangfire storage (AOF + `noeviction`) |
| `OrganizerStore__Provider` | Container | `FileSystem` (default here) or `Blob` |
| `ShardStore__Provider` | Container | `FileSystem` (default here) or `Blob` — selects the highway shard and zoom-index stores |
| `RACE_DATA_DIR` | Compose | Host race-data directory, default `/opt/data/strava/races`, mounted at `/data/races` |
| `SHARD_DATA_DIR` | Compose | Host highway-shard directory, default `/opt/data/strava/shards`, mounted at `/data/shards` |

There is no per-function whitelist. Queues, feeds and timers are discovered by
the plain hosts (`Shared.Hosting`) and assigned to a role by `WORKER_ROLE`.

## Container / role split

| Container | Project | Role | Work |
| --- | --- | --- | --- |
| `api` | `API` + `Backend` | `api` | All API endpoints plus Backend HTTP routes (webhook, token refresh, admin, backfill) on Kestrel |
| `worker-schedule` | `Backend` | `schedule` | 12 Hangfire recurring jobs |
| `worker-heavy` | `Backend` | `heavy` | 12 Hangfire queue consumers + 3 Cosmos change-feed processors |
| `redis` | — | — | Durable Hangfire storage |
| `pmtiles` | `PmtilesJob` | — | Weekly race-tile build (Hangfire scheduler) |

`deploy/scripts/verify-role-coverage.sh` fails CI if the trigger inventory no
longer matches (12 queues / 3 feeds / 12 crons) or if legacy function-list env
vars reappear.

### Race tiles

The `pmtiles` image has tippecanoe compiled in and needs **no Azure connection
strings**: organizer JSON and route GPX come from `RACE_DATA_DIR`
(`/opt/data/strava/races`, mounted at `/data/races`), and the archive is written
to `/data/races/tiles/trails.pmtiles`. The `api`, `worker-schedule` and
`worker-heavy` containers mount the same directory and use the FileSystem store,
so discoveries, scrapes and the weekly build see identical data. The API serves
the archive with HTTP range support.

Highway shards (`{z}/{x}/{y}.pbf`) and zoom indexes (`index/{z}/{x}/{y}.json`)
live in the sibling host directory `SHARD_DATA_DIR` (`/opt/data/strava/shards`,
mounted at `/data/shards` in `api`, `worker-schedule` and `worker-heavy`). Missing
shards are rebuilt from Overpass on first request, so no Blob seed is needed.

```bash
# one-shot build (the container's default is the weekly scheduler)
docker compose run --rm pmtiles build-race-tiles-from-organizers
```

One-time rollout seeds the host directory from Blob; see DEPLOY.md. Tune with
`PMTILES_MEMORY_LIMIT` and `PMTILES_CPUS` in `.env`.

## First-time setup

### 1. Network access to Azure

The stack connects outbound only. The server must reach Cosmos DB, Blob,
Tables and Azure SignalR, plus Strava, Mistral and Overpass. If those resources
use IP firewalls or private endpoints, allow the server's egress IP.
`CosmosConnectionMode=Gateway` is the default because it tolerates container
networking better than Direct mode.

### 2. Provision the server

On a Docker host (Engine + Compose plugin + rsync already installed), with the
repository cloned to `/opt/repos/strava`:

```bash
sudo mkdir -p /opt/repos && sudo chown "$USER" /opt/repos
git clone <this repository> /opt/repos/strava
cd /opt/repos/strava
sudo ./deploy/scripts/bootstrap-server.sh
```

The bootstrap copies `deploy/` to `/opt/stacks/strava` and generates `.env` from
`.env.example`. The repository stays in `/opt/repos/strava`;
`/opt/stacks/strava` is the runtime directory GitHub Actions keeps in sync.

Ansible users can install the base packages and firewall rules with
`deploy/ansible/playbook.yml` (see `deploy/ansible/inventory.example.ini`); it
uploads `deploy/.env`, so create that on the controller first.

### 3. Fill in `.env`

Edit `/opt/stacks/strava/.env` (or `deploy/.env` before Ansible) and replace all
`REPLACE` values:

- `IMAGE_NAMESPACE`: lowercase GitHub owner publishing the GHCR packages.
- `CosmosDBConnection`, `AzureSignalRConnectionString`. Blob and Functions storage
  connection strings are no longer needed — all file data is on local disk.
- `StravaClientSecret`, `stravaVerifyToken`, Mistral settings.
- `AdminApiKey`: generated by `init-env.sh` during bootstrap; change it if desired.

`CosmosAccountResourceId` can stay empty — live RU reporting then falls back to
"unavailable" because the host has no Azure identity. `REDIS_CONNECTION_STRING`
defaults to `redis:6379,abortConnect=false` and only needs changing for an
external Redis. Keep `COMPOSE_PROFILES=` empty for the first deployment.

### 4. Allow the server to pull private images

If the GHCR packages are private, log in once on the server with a read-only
personal access token:

```bash
docker login ghcr.io -u <github-user>
```

### 5. Start the stack

```bash
cd /opt/stacks/strava
docker compose up -d --wait
docker compose ps
./scripts/smoke-test.sh
```

At this point `api` serves all HTTP routes on the internal network; no workers
are running timers or consuming queues.

### 6. Expose the API publicly

Tailscale Funnel forwards 443 on the node to the API's loopback port. Tailnet
preparation (MagicDNS, HTTPS certificates, the `funnel` node attribute) is in
[`docs/tailscale-funnel-runbook.md`](../docs/tailscale-funnel-runbook.md).
The public URL becomes `https://<machine>.<tailnet>.ts.net`; remember to update
the Strava webhook callback and any MCP/Trailscope API base URLs when cutting
clients over.

### Local build (optional)

The production compose file only pulls images. To build locally while still
using the Azure backing services:

```bash
cd deploy
cp .env.example .env
# then fill in the Azure values
docker compose -f docker-compose.yml -f docker-compose.local.yml \
  --profile workers --profile schedules up -d --build
```

## GitHub Actions automatic deploy

`.github/workflows/selfhosted-deploy.yml` runs `verify-role-coverage.sh`, builds
the `api`, `backend` and `pmtiles` images from the plain-host Dockerfiles,
pushes them to GHCR tagged with the commit SHA and `latest`, connects the runner
to the tailnet as an ephemeral `tag:ci` node, syncs `deploy/` to the server over
Tailscale SSH (excluding `.env`, `backups/` and the Ansible inventory), then
pulls and restarts the stack.

Tailscale SSH authenticates the deploy connections — no SSH keys are stored in
GitHub. The server must be enrolled as a tagged node with Tailscale SSH enabled,
and the tailnet policy must allow `tag:ci → tag:server`.

Configure a GitHub Environment named `self-hosted` with these secrets:

| Secret | Meaning |
| --- | --- |
| `SELFHOSTED_HOST` | Server's Tailscale IP (100.x.y.z) or MagicDNS name |
| `SELFHOSTED_USER` | SSH user with access to Docker; list it in the tailnet `ssh` rule `users` |
| `TS_OAUTH_CLIENT_ID` | Tailscale OAuth client ID (writable `auth_keys` scope, tag `tag:ci`) |
| `TS_OAUTH_SECRET` | Tailscale OAuth client secret |

The deployment script (`/opt/stacks/strava/scripts/deploy.sh`) writes the new
image tag to `.env`, waits for health checks, and rolls back to the previous tag
if startup fails.

```bash
# manual rollback / redeploy of an existing tag
/opt/stacks/strava/scripts/deploy.sh <previous-commit-sha>
```

The workflow builds `linux/amd64` images for consistency with the server
architecture. On an ARM host Docker runs them through emulation.

## Rollout order

### Step 1 — verify the self-hosted API in parallel

Leave `COMPOSE_PROFILES=` empty. Test the self-hosted API from the home network
or through an SSH tunnel. The Azure API Function App keeps serving external
traffic (including the Strava webhook) until clients are pointed at the Funnel
URL.

### Step 2 — move the workers

There are no competing consumers now: Service Bus is gone and Hangfire storage
is on the home Redis, which Azure Functions cannot reach. To take over work:

1. Disable the matching worker functions in the Azure Backend app (or stop the
   app) so nothing produces duplicate side effects.
2. Set `COMPOSE_PROFILES=workers` and `docker compose up -d --wait`.

```bash
# edit /opt/stacks/strava/.env
COMPOSE_PROFILES=workers
cd /opt/stacks/strava && docker compose up -d --wait
```

The 3 Cosmos change feeds still share lease containers with Azure. Leave both
sides running only while you accept lease hand-off on restarts; otherwise
disable the Azure-side triggers before taking 100% of a feed. See the
change-feed cutover recipe in `specs/functions-runtime-exit.md`.

### Step 3 — move the scheduled jobs

Timers are not competing consumers: if both Azure and the self-hosted worker
run, each cron fires twice. Disable the matching timer functions in Azure
**before** enabling the profile:

```bash
COMPOSE_PROFILES=workers,schedules
```

### Step 4 — later phases

- Phase 4: wire the remaining Redis caching (Backend geocoding cache → Redis,
  cache-aside on hot reads, precomputed read models).
- Phase 5: add Postgres when read offloading is implemented, together with a
  host backup job and a tested restore.
- Phase 6+: dual-write and the final Cosmos cutover.

## Configuration notes

- Connection strings and secrets come from `/opt/stacks/strava/.env` via Compose
  `env_file`; they are never baked into images.
- `WORKER_ROLE` selects which plain-host role a container runs. The `api`
  container serves both API and Backend HTTP routes.
- `REDIS_CONNECTION_STRING` points at durable Hangfire storage. Do not switch it
  to a cache-style eviction policy — queued job state must never be evicted.
- The stack is internal except for the Funnel front door. Use an SSH tunnel for
  host-level diagnostics.

## Monitoring

- Host and container metrics: Beszel (deployed separately on the server).
- Logs: `docker compose logs -f --tail=200`, with json-file rotation at
  20 MB × 5 per container.
- Cosmos RU usage: Azure portal / Azure Monitor until the migration baseline is
  captured. Hangfire queue depth and retry/failure counts are the replacement
  for the old Service Bus metrics.

## Security checklist

- SSH key authentication only; disable password login in `sshd_config`.
- GitHub Actions deploys go through the Tailscale tailnet from an ephemeral
  `tag:ci` node, authenticated by Tailscale SSH rather than SSH keys.
- Only `127.0.0.1:8080` is published to the host; the public path is Tailscale
  Funnel. No other container publishes host ports.
- Keep `.env` at mode 600 and out of git.
- Protect the `self-hosted` GitHub Environment with required reviewers if you
  want manual approval before production deploys.
