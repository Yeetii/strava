using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using System.Net;
using BAMCIS.GeoJSON;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services;

public class AdminBoundariesCollectionClient(Container container, ILoggerFactory loggerFactory, OverpassClient overpassClient)
    : CollectionClient<StoredFeature>(container, loggerFactory)
{
    private const string SimplificationStepProperty = "geometrySimplificationStep";
    private const string SimplificationEpsilonProperty = "geometrySimplificationEpsilon";
    private const string SimplifiedGeometryProperty = "geometrySimplified";

    private readonly OverpassClient _overpassClient = overpassClient;
    private const string Kind = FeatureKinds.AdminBoundary;

    public async Task<IEnumerable<StoredFeature>> FetchByTiles(IEnumerable<(int x, int y)> keys, int adminLevel, int zoom = 6, bool followPointers = false, CancellationToken cancellationToken = default)
    {
        if (!keys.Any())
            return [];

        var docs = (await QueryByListOfKeys(keys, zoom, adminLevel, cancellationToken)).ToList();
        var keysInDocs = new HashSet<(int x, int y)>(docs.Select(d => (d.X, d.Y)));
        var missingTiles = keys.Where(k => !keysInDocs.Contains((k.x, k.y)));

        foreach (var (x, y) in missingTiles)
            docs.AddRange(await FetchMissingTile(x, y, zoom, adminLevel, cancellationToken));

        var visibleDocuments = docs
            .Where(d => !d.Id.StartsWith("empty-"))
            .ToList();

        if (followPointers)
            visibleDocuments = (await ResolvePointers(visibleDocuments, cancellationToken)).ToList();

        return visibleDocuments
            .OrderBy(d => StoredFeature.IsPointerDocument(d))
            .DistinctBy(d => d.LogicalId);
    }

    private async Task<IEnumerable<StoredFeature>> QueryByListOfKeys(IEnumerable<(int x, int y)> keys, int zoom, int adminLevel, CancellationToken cancellationToken)
    {
        var keyConditions = string.Join(" OR ", keys.Select((_, i) => $"(c.x = @x{i} AND c.y = @y{i})"));
        var queryDefinition = new QueryDefinition($"SELECT * FROM c WHERE ({keyConditions}) AND c.zoom = @zoom AND c.kind = @kind AND c.properties.adminLevel = @adminLevel")
            .WithParameter("@zoom", zoom)
            .WithParameter("@kind", Kind)
            .WithParameter("@adminLevel", adminLevel.ToString());
        int index = 0;
        foreach (var (x, y) in keys)
        {
            queryDefinition = queryDefinition
                .WithParameter($"@x{index}", x)
                .WithParameter($"@y{index}", y);
            index++;
        }
        return await ExecuteQueryAsync<StoredFeature>(queryDefinition, cancellationToken: cancellationToken);
    }

    private async Task<IEnumerable<StoredFeature>> FetchMissingTile(int x, int y, int zoom, int adminLevel, CancellationToken cancellationToken)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);

        // Phase 1: lightweight tags-only query to discover which boundaries are in this tile.
        var discoveredIds = (await _overpassClient.GetAdminBoundaryIds(southWest, northEast, adminLevel, cancellationToken: cancellationToken)).ToList();
        if (discoveredIds.Count == 0)
        {
            var empty = new StoredFeature(Kind, x, y, zoom)
            {
                Id = $"empty-{Kind}-{adminLevel}-{zoom}-{x}-{y}",
            };
            empty.Properties["adminLevel"] = adminLevel.ToString();
            await UpsertBoundaryDocument(empty, cancellationToken);
            return [empty];
        }

        // Phase 2: check Cosmos for boundaries we already have stored.
        var candidateDocIds = discoveredIds
            .Select(d => $"{Kind}:{adminLevel}:{d.osmId}")
            .ToList();
        var existingDocs = (await GetByIdsAsync(candidateDocIds, cancellationToken))
            .ToDictionary(d => d.Id, StringComparer.Ordinal);

        // Phase 3: return already-stored boundaries; create pointers for non-centroid tiles.
        var features = new List<StoredFeature>();
        var missingIds = new List<(string osmType, long osmId)>();

        foreach (var (osmType, osmId, _) in discoveredIds)
        {
            var docId = $"{Kind}:{adminLevel}:{osmId}";
            if (existingDocs.TryGetValue(docId, out var existing))
            {
                // For countries, only include features whose centroid is on this tile.
                if (adminLevel != 2 || (existing.X == x && existing.Y == y))
                    features.Add(existing);
                if (existing.X != x || existing.Y != y)
                {
                    var pointer = StoredFeature.CreatePointer(
                        Kind,
                        existing.FeatureId ?? existing.LogicalId,
                        x, y, zoom,
                        existing.X, existing.Y, existing.Zoom,
                        existing.Id,
                        new Dictionary<string, dynamic> { ["adminLevel"] = adminLevel.ToString() });
                    features.Add(pointer);
                    await UpsertBoundaryDocument(pointer, cancellationToken);
                }
            }
            else
            {
                missingIds.Add((osmType, osmId));
            }
        }

        // Phase 4: fetch full geometry only for genuinely new boundaries.
        if (missingIds.Count > 0)
        {
            var rawFeatures = (await _overpassClient.GetAdminBoundariesByIds(missingIds, cancellationToken)).ToList();

            if (adminLevel == 2)
                rawFeatures = DeduplicateByCountryCode(rawFeatures);

            var newDocuments = rawFeatures
                .SelectMany(feature =>
                {
                    var stored = new StoredFeature(feature, Kind, zoom);
                    stored.Id = $"{Kind}:{adminLevel}:{stored.FeatureId}";
                    stored.Properties["adminLevel"] = adminLevel.ToString();
                    if (adminLevel == 2)
                        SetCountryCodeProperty(stored);
                    var documents = new List<StoredFeature> { stored };

                    if (feature.Geometry is not Point && (stored.X != x || stored.Y != y))
                    {
                        documents.Add(StoredFeature.CreatePointer(
                            Kind,
                            stored.FeatureId ?? stored.LogicalId,
                            x, y, zoom,
                            stored.X, stored.Y, stored.Zoom,
                            stored.Id,
                            new Dictionary<string, dynamic> { ["adminLevel"] = adminLevel.ToString() }));
                    }

                    return documents;
                })
                .ToList();

            await UpsertBoundaryDocuments(newDocuments, cancellationToken);

            // For countries, don't return the full-geometry doc on a non-centroid tile;
            // the pointer already tells the frontend where to find it.
            if (adminLevel == 2)
                newDocuments = newDocuments
                    .Where(d => StoredFeature.IsPointerDocument(d) || d.X == x && d.Y == y)
                    .ToList();

            features.AddRange(newDocuments);
        }

        // If nothing survived (e.g. all boundaries were filtered), store an empty marker.
        if (features.Count == 0)
        {
            var empty = new StoredFeature(Kind, x, y, zoom)
            {
                Id = $"empty-{Kind}-{adminLevel}-{zoom}-{x}-{y}",
            };
            empty.Properties["adminLevel"] = adminLevel.ToString();
            features.Add(empty);
            await UpsertBoundaryDocument(empty, cancellationToken);
        }

        return features;
    }

    /// <summary>
    /// Extracts the ISO 3166-1 alpha-2 country code from a Feature's OSM tags
    /// and stores it under a camelCase-safe key so it survives Cosmos serialization.
    /// </summary>
    private static void SetCountryCodeProperty(StoredFeature stored)
    {
        foreach (var key in new[] { "ISO3166-1", "ISO3166-1:alpha2", "country_code_iso3166_1_alpha_2" })
        {
            if (stored.Properties.TryGetValue(key, out var val))
            {
                var s = val?.ToString();
                if (!string.IsNullOrWhiteSpace(s))
                {
                    stored.Properties["countryCode"] = s;
                    return;
                }
            }
        }
    }

    /// <summary>
    /// When multiple admin_level=2 relations share an ISO3166-1 code (e.g. France +
    /// overseas territories), keep only the one with the largest geometry so the
    /// client sees one feature per country code.  Features without a country code
    /// are always kept.
    /// </summary>
    private static List<Feature> DeduplicateByCountryCode(List<Feature> features)
    {
        static string? GetCountryCode(Feature f)
        {
            if (f.Properties == null) return null;
            // OSM tags: "ISO3166-1", "ISO3166-1:alpha2", or "country_code_iso3166_1_alpha_2"
            foreach (var key in new[] { "ISO3166-1", "ISO3166-1:alpha2", "country_code_iso3166_1_alpha_2" })
            {
                if (f.Properties.TryGetValue(key, out var val))
                {
                    var s = val?.ToString();
                    if (!string.IsNullOrWhiteSpace(s)) return s;
                }
            }
            return null;
        }

        static int EstimateGeometrySize(Feature f)
        {
            return f.Geometry switch
            {
                Polygon p => p.Coordinates.Sum(r => r.Coordinates.Count()),
                MultiPolygon mp => mp.Coordinates.Sum(p => p.Coordinates.Sum(r => r.Coordinates.Count())),
                _ => 0
            };
        }

        var result = new List<Feature>();
        var seenCodes = new Dictionary<string, (Feature Feature, int Size)>(StringComparer.OrdinalIgnoreCase);

        foreach (var f in features)
        {
            var code = GetCountryCode(f);
            if (code == null)
            {
                // Border relations (e.g. "France - Monaco") have no ISO country code.
                // Drop them — only actual sovereign territory polygons belong here.
                continue;
            }

            var size = EstimateGeometrySize(f);
            if (!seenCodes.TryGetValue(code, out var existing) || size > existing.Size)
                seenCodes[code] = (f, size);
        }

        result.AddRange(seenCodes.Values.Select(v => v.Feature));
        return result;
    }

    private async Task UpsertBoundaryDocuments(IEnumerable<StoredFeature> documents, CancellationToken cancellationToken)
    {
        foreach (var document in documents)
        {
            await UpsertBoundaryDocument(document, cancellationToken);
        }
    }

    private async Task UpsertBoundaryDocument(StoredFeature document, CancellationToken cancellationToken)
    {
        if (StoredFeature.IsPointerDocument(document) || document.Id.StartsWith("empty-", StringComparison.Ordinal))
        {
            await UpsertDocument(document, cancellationToken);
            return;
        }

        // Proactively simplify with RDP before the first write attempt.
        // admin_level=2 (countries): 0.01° ≈ 1 km, admin_level=4 (regions): 0.005° ≈ 500 m.
        var adminLevel = document.Properties.TryGetValue("adminLevel", out var lvl) ? lvl?.ToString() : null;
        var epsilon = adminLevel switch
        {
            "2" => 0.01,
            "4" => 0.005,
            _ => 0.005
        };
        var simplified = CreateRdpSimplifiedDocument(document, epsilon);

        try
        {
            await UpsertDocument(simplified, cancellationToken);
            return;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.RequestEntityTooLarge) { }

        // RDP-simplified geometry still too large — fall back to increasingly
        // aggressive nth-point decimation on the already-simplified geometry.
        foreach (var step in new[] { 2, 4, 8, 16, 32 })
        {
            try
            {
                await UpsertDocument(CreateSimplifiedBoundaryDocument(simplified, step), cancellationToken);
                return;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.RequestEntityTooLarge && step < 32)
            {
                continue;
            }
        }

        await UpsertDocument(CreateSimplifiedBoundaryDocument(simplified, 64), cancellationToken);
    }

    private async Task<IEnumerable<StoredFeature>> ResolvePointers(IEnumerable<StoredFeature> documents, CancellationToken cancellationToken)
    {
        var documentList = documents.ToList();
        var pointedIds = documentList
            .Where(StoredFeature.IsPointerDocument)
            .Select(StoredFeature.GetPointerStoredDocumentId)
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (pointedIds.Count == 0)
            return documentList;

        var resolvedDocuments = (await GetByIdsAsync(pointedIds!, cancellationToken))
            .ToDictionary(document => document.Id, StringComparer.Ordinal);

        return documentList
            .Select(document =>
            {
                if (!StoredFeature.IsPointerDocument(document))
                    return document;

                var storedDocumentId = StoredFeature.GetPointerStoredDocumentId(document);
                if (storedDocumentId != null && resolvedDocuments.TryGetValue(storedDocumentId, out var resolved))
                    return resolved;

                return document;
            })
            .Where(document => !StoredFeature.IsPointerDocument(document));
    }

    private static StoredFeature CreateRdpSimplifiedDocument(StoredFeature document, double epsilon)
    {
        return new StoredFeature
        {
            Id = document.Id,
            FeatureId = document.FeatureId,
            Kind = document.Kind,
            X = document.X,
            Y = document.Y,
            Zoom = document.Zoom,
            Geometry = GeometryDecimator.Simplify(document.Geometry, epsilon),
            Properties = new Dictionary<string, dynamic>(document.Properties)
            {
                [SimplifiedGeometryProperty] = true,
                [SimplificationEpsilonProperty] = epsilon
            }
        };
    }

    private static StoredFeature CreateSimplifiedBoundaryDocument(StoredFeature document, int step)
    {
        return new StoredFeature
        {
            Id = document.Id,
            FeatureId = document.FeatureId,
            Kind = document.Kind,
            X = document.X,
            Y = document.Y,
            Zoom = document.Zoom,
            Geometry = GeometryDecimator.Decimate(document.Geometry, step),
            Properties = new Dictionary<string, dynamic>(document.Properties)
            {
                [SimplifiedGeometryProperty] = true,
                [SimplificationStepProperty] = step
            }
        };
    }

    /// <summary>
    /// Fetches countries from Overpass that are not yet stored in Cosmos,
    /// stores them, and creates pointers for all bounding-box tiles.
    /// When <paramref name="countryCodes"/> or <paramref name="osmIds"/> is provided, only those countries are targeted.
    /// Returns a summary of what was fetched and stored.
    /// </summary>
    public async Task<CountryFetchResult> FetchAndStoreCountries(IReadOnlyList<string>? countryCodes, IReadOnlyList<(string osmType, long osmId)>? osmIds, ILogger logger, CancellationToken cancellationToken)
    {
        const int adminLevel = 2;
        const int zoom = 6;

        List<(string osmType, long osmId)> missingIds;
        Dictionary<string, StoredFeature> existingDocs;
        int discoveredCount;
        bool forceRefetch;

        if (osmIds is { Count: > 0 })
        {
            // Direct OSM ID refetch — skip Overpass discovery.
            forceRefetch = true;
            discoveredCount = osmIds.Count;
            missingIds = osmIds.ToList();

            var candidateDocIds = osmIds
                .Select(d => $"{Kind}:{adminLevel}:{d.osmType}:{d.osmId}")
                .ToList();
            existingDocs = (await GetByIdsAsync(candidateDocIds, cancellationToken))
                .ToDictionary(d => d.Id, StringComparer.Ordinal);

            logger.LogInformation("OSM ID refetch: {Count} IDs requested, {Existing} existing docs found",
                osmIds.Count, existingDocs.Count);
        }
        else
        {
            // Phase 1: discover countries via Overpass (tags only, no geometry).
            // When country codes are specified, push the ISO filter into the Overpass query
            // to avoid scanning every admin_level=2 boundary in the world (which times out).
            var worldSw = new Coordinate(-180, -90);
            var worldNe = new Coordinate(180, 90);
            var allDiscovered = (await _overpassClient.GetAdminBoundaryIds(
                worldSw, worldNe, adminLevel,
                isoCodes: countryCodes,
                cancellationToken: cancellationToken)).ToList();
            logger.LogInformation("Overpass discovered {Count} admin_level=2 relations{Filter}",
                allDiscovered.Count,
                countryCodes is { Count: > 0 } ? $" for codes: {string.Join(", ", countryCodes)}" : " globally");

            // Phase 2: if country codes specified, filter to only matching relations.
            if (countryCodes is { Count: > 0 })
            {
                var requestedCodes = new HashSet<string>(countryCodes, StringComparer.OrdinalIgnoreCase);
                allDiscovered = allDiscovered
                    .Where(d => d.tags.TryGetValue("ISO3166-1", out var code) && requestedCodes.Contains(code)
                              || d.tags.TryGetValue("ISO3166-1:alpha2", out code) && requestedCodes.Contains(code)
                              || d.tags.TryGetValue("country_code_iso3166_1_alpha_2", out code) && requestedCodes.Contains(code))
                    .ToList();

                logger.LogInformation("Filtered to {Count} relations matching requested country codes: {Codes}",
                    allDiscovered.Count, string.Join(", ", countryCodes));
            }

            // Phase 3: check which ones are already stored in Cosmos.
            forceRefetch = countryCodes is { Count: > 0 };
            discoveredCount = allDiscovered.Count;
            var candidateDocIds = allDiscovered
                .Select(d => $"{Kind}:{adminLevel}:{d.osmType}:{d.osmId}")
                .ToList();
            existingDocs = (await GetByIdsAsync(candidateDocIds, cancellationToken))
                .ToDictionary(d => d.Id, StringComparer.Ordinal);

            missingIds = forceRefetch
                ? allDiscovered.Select(d => (d.osmType, d.osmId)).ToList()
                : allDiscovered
                    .Where(d => !existingDocs.ContainsKey($"{Kind}:{adminLevel}:{d.osmType}:{d.osmId}"))
                    .Select(d => (d.osmType, d.osmId))
                    .ToList();
        }

        logger.LogInformation("Already stored: {Existing}, to fetch: {Missing}{Force}",
            existingDocs.Count, missingIds.Count, forceRefetch ? " (force-refetch)" : "");

        // Phase 4: fetch full geometry from Overpass in batches BEFORE deleting old data.
        // Batch size 1 when force-fetching by OSM ID: large countries (Russia, Canada, US) can
        // time out Overpass on their own, so batching them together just causes collateral failures.
        var newCountries = new List<StoredFeature>();
        var failedIds = new List<string>();
        if (missingIds.Count > 0)
        {
            int batchSize = forceRefetch ? 1 : 5;
            var batches = missingIds
                .Select((id, i) => (id, i))
                .GroupBy(x => x.i / batchSize)
                .Select(g => g.Select(x => x.id).ToList())
                .ToList();

            for (var bi = 0; bi < batches.Count; bi++)
            {
                var batch = batches[bi];
                logger.LogInformation("Fetching batch {Batch}/{Total} ({Count} countries) from Overpass",
                    bi + 1, batches.Count, batch.Count);

                try
                {
                    var rawFeatures = (await _overpassClient.GetAdminBoundariesByIds(batch, cancellationToken)).ToList();
                    rawFeatures = DeduplicateByCountryCode(rawFeatures);

                    var batchFetched = new List<StoredFeature>();
                    foreach (var feature in rawFeatures)
                    {
                        var stored = new StoredFeature(feature, Kind, zoom);
                        stored.Id = $"{Kind}:{adminLevel}:{stored.FeatureId}";
                        stored.Properties["adminLevel"] = adminLevel.ToString();
                        SetCountryCodeProperty(stored);
                        batchFetched.Add(stored);
                    }

                    var fetchedFeatureIds = new HashSet<string>(batchFetched.Select(c => c.FeatureId!), StringComparer.Ordinal);
                    failedIds.AddRange(batch
                        .Where(m => !fetchedFeatureIds.Contains($"{m.osmType}:{m.osmId}"))
                        .Select(m => $"{m.osmType}:{m.osmId}"));

                    newCountries.AddRange(batchFetched);
                    logger.LogInformation("Batch {Batch}: fetched {Fetched}, failed {Failed}",
                        bi + 1, batchFetched.Count, batch.Count - batchFetched.Count);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Batch {Batch} failed entirely", bi + 1);
                    failedIds.AddRange(batch.Select(m => $"{m.osmType}:{m.osmId}"));
                }
            }

            logger.LogInformation("Fetched {Count} countries from Overpass in {Batches} batches", newCountries.Count, batches.Count);

            if (failedIds.Count > 0)
                logger.LogWarning("{FailedCount} countries failed to fetch from Overpass: {Ids}", failedIds.Count, string.Join(", ", failedIds));
        }

        // Phase 4b: for force-refetch, delete old docs + pointers now that we have new data ready.
        int alreadyStoredCount = existingDocs.Count;
        if (forceRefetch && existingDocs.Count > 0)
        {
            logger.LogInformation("Deleting {Count} existing country docs and their pointers", existingDocs.Count);
            foreach (var doc in existingDocs.Values)
            {
                var pointerQuery = new QueryDefinition(
                    "SELECT * FROM c WHERE c.kind = @kind AND c.properties.isPointer = true AND c.properties.storedDocumentId = @docId")
                    .WithParameter("@kind", Kind)
                    .WithParameter("@docId", doc.Id);
                var pointers = (await ExecuteQueryAsync<StoredFeature>(pointerQuery, cancellationToken: cancellationToken)).ToList();
                foreach (var pointer in pointers)
                {
                    var pk = new PartitionKeyBuilder().Add((double)pointer.X).Add((double)pointer.Y).Build();
                    await DeleteDocument(pointer.Id, pk, cancellationToken);
                }
                logger.LogInformation("Deleted {PointerCount} pointers for {DocId}", pointers.Count, doc.Id);

                var docPk = new PartitionKeyBuilder().Add((double)doc.X).Add((double)doc.Y).Build();
                await DeleteDocument(doc.Id, docPk, cancellationToken);
            }
            existingDocs.Clear();
        }

        // Phase 5: store new countries.
        foreach (var stored in newCountries)
        {
            await UpsertBoundaryDocument(stored, cancellationToken);
            string countryCode = stored.Properties.TryGetValue("countryCode", out var cc) ? cc?.ToString() ?? "?" : "?";
            logger.LogInformation("Stored country: {Id} ({CountryCode}) at tile ({X},{Y})",
                stored.Id, countryCode, stored.X, stored.Y);
        }

        // Phase 6: create pointers for newly fetched countries covering their bounding-box tiles.
        var pointersCreated = 0;
        var maxTile = (1 << zoom) - 1;

        var countryIndex = 0;
        foreach (var country in newCountries)
        {
            countryIndex++;
            if (country.Geometry is Point)
                continue;

            string cName = country.Properties.TryGetValue("countryCode", out var cVal) ? cVal?.ToString() ?? country.Id : country.Id;
            var tiles = AdminBoundaryMetricsEnricher.CalculateCandidateTiles(country.Geometry, zoom, borderSamplingStep: 10)
                .Where(t => t.x >= 0 && t.x <= maxTile && t.y >= 0 && t.y <= maxTile)
                .Where(t => t.x != country.X || t.y != country.Y)
                .ToList();

            logger.LogInformation("Creating {TileCount} pointers for {Country} ({Index}/{Total})",
                tiles.Count, cName, countryIndex, newCountries.Count);

            foreach (var (tx, ty) in tiles)
            {
                var pointer = StoredFeature.CreatePointer(
                    Kind,
                    country.FeatureId ?? country.LogicalId,
                    tx, ty, zoom,
                    country.X, country.Y, country.Zoom,
                    country.Id,
                    new Dictionary<string, dynamic> { ["adminLevel"] = adminLevel.ToString() });
                await UpsertBoundaryDocument(pointer, cancellationToken);
                pointersCreated++;
            }
        }

        logger.LogInformation("Created {Pointers} pointers for {Countries} new countries", pointersCreated, newCountries.Count);

        return new CountryFetchResult(
            Discovered: discoveredCount,
            AlreadyStored: alreadyStoredCount,
            NewlyFetched: newCountries.Count,
            PointersCreated: pointersCreated,
            Failed: failedIds.Count);
    }

    public record CountryFetchResult(int Discovered, int AlreadyStored, int NewlyFetched, int PointersCreated, int Failed);

    public async Task<int> DeleteAllBoundariesAsync(CancellationToken cancellationToken = default)
    {
        var queryDefinition = new QueryDefinition(
            "SELECT c.id, c.x, c.y FROM c WHERE c.kind = @kind")
            .WithParameter("@kind", Kind);

        var docs = (await ExecuteQueryAsync<StoredFeature>(queryDefinition, cancellationToken: cancellationToken)).ToList();

        foreach (var doc in docs)
        {
            var partitionKey = new PartitionKeyBuilder()
                .Add((double)doc.X)
                .Add((double)doc.Y)
                .Build();
            await DeleteDocument(doc.Id, partitionKey, cancellationToken);
        }

        return docs.Count;
    }

    /// <summary>
    /// Wipes all cached docs (main, pointers, empty markers) for a specific tile + admin level,
    /// then re-fetches from Overpass and stores the result.
    /// Returns the fresh features for the tile.
    /// </summary>
    public async Task<IEnumerable<StoredFeature>> RefreshTile(int x, int y, int adminLevel, int zoom, CancellationToken cancellationToken)
    {
        // Delete everything stored for this tile at this admin level.
        var queryDefinition = new QueryDefinition(
            "SELECT c.id, c.x, c.y FROM c WHERE c.kind = @kind AND c.x = @x AND c.y = @y AND c.zoom = @zoom AND c.properties.adminLevel = @adminLevel")
            .WithParameter("@kind", Kind)
            .WithParameter("@x", x)
            .WithParameter("@y", y)
            .WithParameter("@zoom", zoom)
            .WithParameter("@adminLevel", adminLevel.ToString());

        var existing = (await ExecuteQueryAsync<StoredFeature>(queryDefinition, cancellationToken: cancellationToken)).ToList();
        foreach (var doc in existing)
        {
            var pk = new PartitionKeyBuilder().Add((double)doc.X).Add((double)doc.Y).Build();
            await DeleteDocument(doc.Id, pk, cancellationToken);
        }

        return await FetchMissingTile(x, y, zoom, adminLevel, cancellationToken);
    }
}
