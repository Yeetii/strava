# Job epoch rediscovery
Branch: main
Delivery strategy: uncommitted work units reported for review; no branch/PR requested
Test runner: dotnet test

## Specs

S1: "i must be able to mark a type of job for rediscovery, probably by an admin endpoint. this would probably reset these timers and the processing status so the polling rediscovers them. grill me on how to do this best"

S2: "keep the current epoch in a table for all types of jobs (or is there a better way?). add an admin edpoint to bump the epoch of a given job type. remove any stale similar admin endpoints. show me what the data model would be for this solution. grill me some more."

S3: "1. Conflict should mean overwrite 2. adminBoundaryMetricsVersion would no longer be needed? ForceEnrichProperty can be deleted 3. They are not used 4. don't care 5. yes seed to 1"

S4: "the bools inside of ActivityProcessingStatus could be replaced with the epoch number, a null or 0 means it hasn't been run yet"

S5: "well the polling wouldn't mark the epoch but the worker. by the time of the polling it doesn't matter. no need for the DoneAtUtc == null. marker older than 24h is needed when epoc is low too"

S6: "\"Conflict should mean overwrite\" does not fix stale rows - Yes you're right, make sure to delete old rows"

S7: "Seed-to-1 is a full backfill on deploy - that is fine with a complete backfill"

## Tasks

T1 (S2,S3,S7): Schema — `job_epochs` table with four seeded types at epoch 1; `admin_boundaries.metrics_generation`; idempotent migration file | route: inline | status: done (01-init.sql, 04-add-job-epochs.sql)
T2 (S4,S5): Replace stage bools in `ActivityProcessingStatus` with per-stage epoch numbers; carry claimed epoch in job payloads | route: inline | status: done (Activity.cs, JobContracts.cs; kept DoneAtUtc and SportTypeExcluded)
T3 (S5): Epoch-aware poll selection for both activity and admin-boundary polls; keep the 24h marker gate | route: inline | status: done (BackendAdminQueries.cs)
T4 (S5,S6): Workers write the claimed epoch and clear the marker on completion; purge old junction rows before recomputing; boundary worker drops version/force gating | route: inline | status: done (UserSyncStatusService, all three workers, AdminBoundaryRepository, DomainRules)
T5 (S1,S2,S3): Admin endpoint to bump a job type's epoch; remove the three stale queue/reEnrich endpoints | route: inline | status: done (BumpJobEpoch.cs, EndpointMappings.cs)
T6 (all): RED/GREEN integration + worker tests and focused/full suite verification | route: inline | status: done (Core 294, Workers 370, Pmtiles 32)

## Log

L1: User request as quoted in S1, followed by a multi-turn design interview. Decisions reached: generation counter in a table (not mass reset); epoch replaces stage bools; worker (not poll) writes the epoch; marker older than 24h gates even low epochs; stale junction rows deleted before recompute; epoch table seeded to 1 with the resulting full backfill explicitly accepted; audit fields not needed; version/force-enrich removed; three unused admin endpoints removed.

L2: Selection rule settled as `COALESCE(stageEpoch, 0) < current_epoch AND (enqueuedAtUtc IS NULL OR enqueuedAtUtc < NOW() - INTERVAL '24 hours')`. Completion writes the claimed epoch and clears the marker; DoneAtUtc fields are redundant. Bumping mid-flight therefore rediscovers on the next poll rather than waiting 24h.

L3: Corrected the user's "conflict = overwrite" premise: `ON CONFLICT DO UPDATE` cannot remove rows absent from the new result set, so a code rework would leave stale links. Junction rows are deleted per activity before recompute.

L4: Route: inline — one coherent behavior with coupled edits across model, queries, workers and endpoint; no independent parallel units, so no writer delegation. Risk: item 1 (schema + persisted format change, additive only) and item 3 (public admin endpoint changes); verification by integration tests and full suites.

L5: T1-T6 implemented. Seed-to-1 means the first poll after migration reprocesses every activity stage and every admin boundary, explicitly accepted by the user.

L6: Deviations from the design interview: kept SummitedPeaksDoneAtUtc/VisitedPathsDoneAtUtc/VisitedAreasDoneAtUtc as informational completion timestamps (not used in selection) and kept SportTypeExcluded as the exclusion reason flag. Unversioned manual enqueues carry epoch 0; the status service treats a stage with a stored epoch > 0 as already done for those, avoiding spurious count increments. Existing junction writers already called RemoveByActivity before AddRange, so the purge requirement was already satisfied and was verified rather than added.

L7: Verification: solution builds clean; PostGIS integration tests 32/32 (added epoch-selection, bump, missing-row, and marker-clearing cases); Core 294/294, Workers 370/370, Pmtiles 32/32. RED was observed for the queued-at marker tests in the preceding task; this task's new epoch tests were written alongside implementation rather than preceded by a RED run.
