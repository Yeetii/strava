# Trailscope User Sync

This document describes the user file/settings sync flow between the Trailscope frontend and this API.

## Scope

- Frontend repo: `/Users/erik/Code/Erik/trailscope/website`
- API repo: this repo
- Transport: cookie-authenticated HTTP calls to `/api/userSync`
- Storage: Cosmos DB container `userSyncItems`
- Conflict rule: highest `updatedAt` timestamp wins

## Endpoints

- `GET /api/userSync`
  Returns the authenticated user's current sync snapshot.
- `POST /api/userSync`
  Accepts local changes and returns the merged authoritative snapshot.
- `OPTIONS /api/userSync`
  Preflight/CORS handling for environments that require it.

The API uses a single Azure Function for all three methods because Azure Functions rejected separate `GET` and `POST` functions on the same route.

Implementation: `API/Endpoints/User/GetPostUserSync.cs`

## Auth

Sync uses the existing `session` cookie created by `POST /{authCode}/login`.

- No bearer token is used.
- Missing or invalid session returns `401`.
- The frontend only attempts sync when its `activeSession` store is `true`.

## Cosmos Model

Each sync item is stored as one Cosmos document in `userSyncItems`.

Model: `Shared/Models/UserSyncItem.cs`

Document shape:

```json
{
  "id": "userSync:file:12345:abc-sync-id",
  "documentType": "userSync",
  "userId": "12345",
  "category": "file",
  "key": "abc-sync-id",
  "updatedAt": 1712730000000,
  "deleted": false,
  "value": { "...": "raw JSON payload" }
}
```

Important details:

- `category` is either `setting` or `file`.
- `key` is the stable logical key within that category.
- `deleted=true` is a tombstone.
- `value` is stored in Cosmos as raw JSON text via `RawJsonStringConverter`, not as a live `JsonElement` field in memory.

## Why Raw JSON Is Used

`JsonElement` instances coming back from Cosmos/serializer pipelines caused invalid-object-state failures during later serialization.

To avoid that:

- `UserSyncItem.ValueJson` stores raw JSON text in memory.
- `RawJsonStringConverter` reads the JSON payload as raw text and writes it back as JSON.
- `UserSyncService` parses raw JSON text into a fresh `JsonDocument` only when creating API responses.

Files:

- `Shared/Serialization/RawJsonStringConverter.cs`
- `Shared/Services/UserSyncService.cs`

## Merge Semantics

Service: `Shared/Services/UserSyncService.cs`

Algorithm:

1. Load all existing sync items for the authenticated user.
2. Group incoming entries by key.
3. Keep the newest incoming entry per key.
4. Compare incoming `updatedAt` with the currently stored document.
5. Ignore older incoming entries.
6. Upsert newer incoming entries.
7. Return the full merged snapshot.

Delete behavior:

- Deletes are represented by tombstones.
- A delete wins over older non-delete data if its timestamp is newer.
- This is required so deletions propagate across devices.

## Frontend Model

The frontend keeps IndexedDB as the working store and syncs it opportunistically.

Main files in Trailscope:

- `src/lib/logic/cosmos-sync.ts`
- `src/lib/logic/sync-metadata.ts`
- `src/lib/logic/file-action-manager.ts`
- `src/lib/logic/settings.ts`

Local state:

- Files live in Dexie tables `fileids`, `files`, `patches`.
- Settings live in Dexie table `settings`.
- Sync bookkeeping lives in `settings['__cosmosSyncMetadata']`.

Metadata shape:

- `lastSyncedAt`
- per-setting timestamps
- per-file timestamps
- file tombstones

## Stable File Identity

Frontend local file IDs like `gpx-0` are device-local and cannot be used as cross-device sync IDs.

The frontend therefore maintains:

- local file ID: used only inside the current IndexedDB/UI session
- sync file ID: stable cross-device identifier stored on `file._data.syncId`

This is required so multiple devices do not overwrite each other's unrelated files that happen to share local IDs.

## Setting Translation

Some settings contain file references and cannot be synced as-is.

Translated settings:

- `fileOrder`
- `libraryData`

The frontend translates:

- local file IDs -> sync IDs before upload
- sync IDs -> local file IDs after download

Without this translation, synced settings would point at non-existent local files on other devices.

## Startup Flow

Frontend startup:

1. Root layout initializes runtime config.
2. Root layout starts `cosmosSyncManager`.
3. App page connects Dexie-backed settings and file state.
4. When `activeSession` becomes `true`, sync schedules an initial pull.

Important detail:

- Sync must start after config initialization, otherwise it may use the wrong API URL.

## Request Flow

Initial sync:

1. Seed missing local metadata for existing files/settings.
2. `GET /api/userSync`
3. Apply remote snapshot into IndexedDB.
4. Update `lastSyncedAt`.

Incremental sync:

1. Collect locally dirty settings/files using metadata timestamps.
2. `POST /api/userSync` with only changed entries.
3. Apply merged snapshot returned by the server.
4. Advance `lastSyncedAt`.

## CORS And Local Dev

Local Azure Functions CORS handling can intercept `OPTIONS` before custom endpoint code runs.

Important frontend behavior:

- The sync `POST` intentionally does not set an explicit `Content-Type: application/json` header.
- This keeps the request in the browser's simple-request path and avoids problematic local preflight behavior with credentials.

Important backend behavior:

- `CorsHeaders` adds `Access-Control-Allow-Credentials` and echoes the request origin when available.

## Retry Behavior

Frontend sync uses retry backoff.

- initial retry: 5 seconds
- max retry: 60 seconds
- reset on successful pull, successful push, or clean no-op cycle

This avoids tight retry loops when sync is temporarily unavailable.

## Cancellation Behavior

Some API endpoints are user-driven and frequently cancelled by the browser while panning or navigating.

Shared helper:

- `API/Utils/RequestCancellation.cs`

Behavior:

- treat `OperationCanceledException` with a cancelled request token as normal request cancellation
- return `204 No Content` instead of surfacing noisy exception logs for those endpoints

## Local Dev Gotchas

1. Restart `func start` after backend changes.
   A stale Functions host on port `7071` can keep serving old binaries and make debugging misleading.

2. Ensure the running host picked up `CosmosConnectionMode`.
   The API now reads `CosmosConnectionMode` from local settings and applies it to the Cosmos SDK client.

3. If `GET /api/userSync` fails while other authenticated endpoints work, inspect `userSyncItems` serialization first.

4. If all Cosmos-backed endpoints hang but `/api/health` responds, suspect the running Functions process or Cosmos client configuration before suspecting the endpoint code.

## Files To Read First

If an agent needs to work on sync again, start here:

- `API/Endpoints/User/GetPostUserSync.cs`
- `Shared/Services/UserSyncService.cs`
- `Shared/Models/UserSyncItem.cs`
- `Shared/Serialization/RawJsonStringConverter.cs`
- Trailscope: `src/lib/logic/cosmos-sync.ts`
- Trailscope: `src/lib/logic/sync-metadata.ts`

## Non-Goals

- No per-field merge. Entire item is last-write-wins.
- No conflict UI. The latest timestamp silently wins.
- No sync for Overpass tile caches or other derived caches.