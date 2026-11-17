# Copilot Instructions

## Cosmos DB Performance

Documents in our Cosmos containers can be very large (multi-thousand lines). Always prefer:
- **Patch operations** (`PatchDocument` / `PatchOperation.Set` / `PatchOperation.Remove`) over full document upserts when updating a few fields.
- **Projected queries** (`SELECT c.id, c.x, c.y, c.properties.foo`) over `SELECT *` when only a subset of fields is needed.

## Bruno API Collection

All API endpoints must have a corresponding Bruno request file in the `bruno/` folder.
When adding, renaming, or removing an endpoint in the API project, update the matching `.bru` file under the appropriate subfolder (e.g. `bruno/^manage/` for admin endpoints).

## Queue Retries

All queues run on Hangfire/Redis. `QueueJobDispatcher.Execute` carries
`[AutomaticRetry(Attempts = 5, DelaysInSeconds = [60, 300, 900, 1800, 3600])]`.
Worker bodies should let exceptions propagate so Hangfire owns retries and the
dead-letter list. There is no per-queue backpressure service and no SDK message
settlement: `QueueMessageGuard.HasLockToken` only guards synthetic messages.

Keep Cosmos hot-path checks lightweight; avoid Azure Monitor queries during
worker execution.

## Backfill Pattern

When a new field needs to be added to existing Cosmos documents:
1. Add the field to the model as nullable so existing documents remain valid.
2. Write the field on all new/patched documents in the worker going forward.
3. Create a backfill Azure Function (HTTP POST) in `Backend/` that:
   - Queries for documents missing the field (`WHERE NOT IS_DEFINED(c.field) OR IS_NULL(c.field)`).
   - Collects `activityIds` from those documents and re-queues them onto the worker's Hangfire queue via `QueueActivityCollectionJobs.QueueActivityIdsAsync`.
   - Supports pagination via `continuationToken` query param + `ExecuteQueryPageAsync`.
   - Has both a single-page variant and an `/all` variant that loops until done.
4. Add Bruno request files for both variants under `bruno/^manage/workers/`.

**Before running a backfill locally:** the backfill enqueues onto the same Hangfire queue the production worker processes. Disable or stop the remote (production) heavy worker first so it cannot consume jobs before the local worker can.

See `BackfillVisitedPathTileIndex` and `BackfillVisitedPathOsmHighwayIds` in `Backend/` as reference implementations.

## Visited Path Identity

Visited path identity must be based on **OSM way id** (`osmId`), not on shard-local feature ids.

- In `VisitedPathsWorker`, treat `osmId` as canonical for dedupe and persistence.
- `VisitedPath.Id` should be `"{userId}-{osmId}"`
- Never persist shard-scoped hashed feature ids as visited-path identity. They vary by tile/shard and cause duplicate visited paths for the same OSM way.
- Frontend matching should use `osmId` for stable joins across zoom levels and shard boundaries.
