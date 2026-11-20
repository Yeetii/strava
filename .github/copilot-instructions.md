# Copilot Instructions

## Bruno API Collection

All API endpoints must have a corresponding Bruno request file in the `bruno/` folder.
When adding, renaming, or removing an endpoint in the API project, update the matching `.bru` file under the appropriate subfolder (e.g. `bruno/^manage/` for admin endpoints).

## Queue Retries

All queues run on Hangfire/Redis. `QueueJobDispatcher.Execute` carries
`[AutomaticRetry(Attempts = 5, DelaysInSeconds = [60, 300, 900, 1800, 3600])]`.
Worker bodies should let exceptions propagate so Hangfire owns retries and the
dead-letter list. There is no per-queue backpressure service and no SDK message
settlement: `QueueMessageGuard.HasLockToken` only guards synthetic messages.

Keep database hot-path checks lightweight; avoid expensive monitoring queries
during worker execution.

## Backfill Pattern

When a new field needs to be added to existing PostgreSQL rows:
1. Add the field to the model as nullable so existing rows remain valid.
2. Write the field on all new/updated rows in the worker going forward.
3. Create a backfill endpoint (HTTP POST) in `Stigar.Api/Endpoints/Backend/` that:
   - Queries for rows missing the field with a SQL predicate.
   - Collects `activityIds` from those rows and re-queues them onto the worker's Hangfire queue via `QueueActivityCollectionJobs.QueueActivityIdsAsync`.
   - Supports keyset pagination via `batchSize` and `afterId`/`continuationToken` query params.
   - Has both a single-page variant and an `/all` variant that loops until done.
4. Add Bruno request files for both variants under `bruno/^manage/workers/`.

**Before running a backfill locally:** the backfill enqueues onto the same Hangfire queue the production worker processes. Disable or stop the remote (production) heavy worker first so it cannot consume jobs before the local worker can.

See `BackfillAdminBoundaryMetrics` and `BackfillVisitedPathOsmHighwayIds` in `Stigar.Api/Endpoints/Backend/` as reference implementations.

## Visited Path Identity

Visited path identity must be based on **OSM way id** (`osmId`), not on shard-local feature ids.

- In `VisitedPathsWorker`, treat `osmId` as canonical for dedupe and persistence.
- `VisitedPath.Id` should be `"{userId}-{osmId}"`
- Never persist shard-scoped hashed feature ids as visited-path identity. They vary by tile/shard and cause duplicate visited paths for the same OSM way.
- Frontend matching should use `osmId` for stable joins across zoom levels and shard boundaries.
