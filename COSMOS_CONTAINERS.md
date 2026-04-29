# Cosmos Container Conventions

This repo uses multiple Cosmos containers with different partition key strategies.
Do not assume that every document can be patched, deleted, or read by using `new PartitionKey(id)`.

## Why This Matters

The recent race assembly TTL bug came from assuming the `races` container was partitioned by `id`.
It is not.

- Race documents are identified by ids like `race:example.com-3`.
- But the `races` container is partitioned by tile coordinates `(x, y)`.
- Item patch/delete calls must therefore use the document's real `(x, y)` partition key, not its `id`.

If you patch with the wrong partition key, the operation will not target the intended document.

## Known Container Partition Keys

### `raceOrganizers`

- Effective partition key: organizer key / document id
- Example id: `example.com` or `tracedetrail.fr~event~x`
- Safe point operations:
  - `new PartitionKey(organizerKey)`
  - `new PartitionKey(id)` when `id == organizerKey`

Code references:

- [Shared/Services/RaceOrganizerClient.cs](/Users/erik/Code/Erik/strava/Shared/Services/RaceOrganizerClient.cs)

### `races`

- Effective partition key: composite `(x, y)`
- Document ids are not the partition key
- Example id: `race:example.com-0`
- Safe point operations require reading or already knowing `x` and `y`

Code references:

- [Shared/Services/RaceCollectionClient.cs](/Users/erik/Code/Erik/strava/Shared/Services/RaceCollectionClient.cs)
- [Tools/Program.cs](/Users/erik/Code/Erik/strava/Tools/Program.cs)

### `osmFeatures`

- Effective partition key: composite tile coordinates `(x, y)`
- This applies to tiled feature caches and admin-boundary related patch/delete flows

Code references:

- [Shared/Services/AdminBoundariesCollectionClient.cs](/Users/erik/Code/Erik/strava/Shared/Services/AdminBoundariesCollectionClient.cs)
- [Shared/Services/TiledCollectionClient.cs](/Users/erik/Code/Erik/strava/Shared/Services/TiledCollectionClient.cs)

### Containers That Commonly Behave Like `/id`

These are used in code with single-value partition keys matching the document identity or other explicit scalar keys:

- `users`
- `sessions`
- `raceOrganizers`

Do not extend this list without checking the actual code path or container definition.

## Rules For Future Cosmos Changes

1. Before adding any point patch/delete/read code, confirm the container's partition key shape.
2. For tiled containers, expect composite `(x, y)` keys and use `PartitionKeyBuilder`.
3. For race docs, never assume `id` is enough for patch/delete operations.
4. If you only know a race `id`, first project `id, x, y`, then patch using `(x, y)`.
5. If you add a new container, document its partition key here when the first write path is introduced.

## Practical Examples

Correct for organizer documents:

```csharp
await container.PatchItemAsync<RaceOrganizerDocument>(
    organizerKey,
    new PartitionKey(organizerKey),
    operations,
    cancellationToken: cancellationToken);
```

Correct for race documents:

```csharp
var pk = new PartitionKeyBuilder()
    .Add((double)x)
    .Add((double)y)
    .Build();

await container.PatchItemAsync<StoredFeature>(
    raceId,
    pk,
    operations,
    cancellationToken: cancellationToken);
```

Incorrect for race documents:

```csharp
await container.PatchItemAsync<StoredFeature>(
    raceId,
    new PartitionKey(raceId),
    operations,
    cancellationToken: cancellationToken);
```