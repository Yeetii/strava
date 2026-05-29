# Service Bus Scheduled Message Explorer - Product Spec

## Problem

Azure Service Bus does not provide a broker API to enumerate scheduled messages with their scheduled enqueue timestamps. Runtime properties expose only counts.

This creates operational gaps:

- We cannot answer "what is scheduled and for when?" from Service Bus directly.
- We cannot safely do scheduled-only purge without sequence numbers.
- We cannot investigate unexpected queue latency caused by delayed scheduled copies.

## Goal

Create an internal explorer for scheduled Service Bus messages by introducing a schedule ledger captured at schedule time.

Primary outcomes:

- Inspect scheduled jobs by queue/topic and time window.
- Know planned enqueue time for each scheduled message.
- Cancel (purge) scheduled messages in a targeted, auditable, idempotent way.

## Non-Goals

- Replacing Service Bus as the source of delivery truth.
- Reconstructing historical scheduled messages that were never recorded.
- Building a generic external product.

## Key Constraints

- Service Bus cannot list scheduled messages directly.
- Cancellation requires sequence number.
- Topic scheduling lives on the topic sender side, not subscriptions.

## Proposed Architecture

1. Wrap all scheduling calls behind one shared component.
2. On schedule success, persist a ledger record with sequence number and metadata.
3. Build explorer query endpoints against the ledger store.
4. Implement purge as batched cancellation by sequence number.
5. Persist cancellation attempts and outcomes for traceability.

## Data Model (Ledger)

Suggested fields:

- `id`: deterministic unique id (for idempotency)
- `namespace`
- `entityType`: `queue` or `topic`
- `entityName`
- `sequenceNumber`
- `scheduledEnqueueUtc`
- `messageId`
- `correlationId`
- `jobType` (optional)
- `createdBy` (service or endpoint)
- `payloadHash` (optional, no raw payload)
- `status`: `scheduled`, `cancelRequested`, `canceled`, `cancelFailed`, `delivered` (optional)
- `createdUtc`
- `updatedUtc`

Store options:

- Preferred: Cosmos container dedicated to operations metadata.
- Alternative: Azure Table Storage.

## Functional Requirements

1. Record each scheduled message after scheduling succeeds.
2. Support listing by:
- namespace
- entity
- status
- scheduled time range
- messageId/correlationId
3. Show summary counts by status and nearest enqueue times.
4. Allow targeted cancel by filter (dry-run first, then execute).
5. Execute cancel in batches with bounded concurrency.
6. Make cancel idempotent and retry-safe.
7. Emit operation logs and metrics for every cancel batch.

## API Surface (Internal)

Candidate endpoints (manage/admin scope):

- `GET /api/manage/sb/scheduled`
- `GET /api/manage/sb/scheduled/summary`
- `POST /api/manage/sb/scheduled/cancel`

`cancel` request should include:

- filter criteria (entity, date range, ids)
- `dryRun` boolean
- `maxItems`
- optional `reason`

`cancel` response should include:

- matched count
- attempted count
- canceled count
- failed count
- failure samples

## UI / Explorer Behavior

- Filter panel for namespace/entity/time/status.
- Results table with sortable `scheduledEnqueueUtc`.
- Batch actions:
- Dry-run cancel
- Execute cancel
- Export selected rows
- Confirmation dialog with explicit blast-radius summary.

## Security and Compliance

- Admin-only access.
- No raw message payload storage by default.
- Audit trail for all cancel actions (who, when, why, filter).
- Protect against broad accidental purges with limits and confirmation.

## Reliability and Performance

- Cancellation worker must use bounded parallelism.
- Partial failures should be reported and retriable.
- Duplicate cancel calls should not corrupt ledger state.
- Use patch-style updates for ledger status transitions when using Cosmos.

## Observability

Track metrics:

- `scheduled_ledger_write_success_total`
- `scheduled_ledger_write_failure_total`
- `scheduled_cancel_attempt_total`
- `scheduled_cancel_success_total`
- `scheduled_cancel_failure_total`
- `scheduled_cancel_latency_ms`

Track structured logs with operation ids and entity scope.

## Rollout Plan

1. Phase 1: Add scheduling wrapper + ledger writes only.
2. Phase 2: Add read-only explorer endpoints and summary views.
3. Phase 3: Add dry-run cancellation.
4. Phase 4: Enable execute cancellation with safeguards.

## Backfill Strategy

- No reliable broker backfill for old scheduled messages.
- Start ledger from rollout date.
- Document this boundary in the explorer UI.

## Testing Strategy

- Unit tests for:
- idempotent ledger writes
- filter building
- cancel batching and retry
- Integration tests for:
- schedule then list
- dry-run and execute cancel flow
- partial cancel failure handling
- Contract tests for admin API schema.

## Repo-Specific Implementation Notes

- If new manage endpoints are added, add matching Bruno requests under `bruno/` per repository conventions.
- Keep cancellation logic separate from worker hot paths to avoid queue-processing regressions.

## Open Questions

1. Preferred ledger store in this repo: Cosmos vs Table Storage?
2. Should we mark `delivered` state, or treat ledger as schedule/cancel only?
3. Should cancel be synchronous for small batches and async job-based for large ones?
4. What default safety cap should be enforced for one cancel operation?
