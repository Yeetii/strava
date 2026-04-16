using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using System.Net;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services;

public class AdminBoundariesCollectionClient(Container container, ILoggerFactory loggerFactory, OverpassClient overpassClient)
    : CollectionClient<StoredFeature>(container, loggerFactory)
{
    private const string SimplificationStepProperty = "geometrySimplificationStep";
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
        var rawFeatures = await _overpassClient.GetAdminBoundaries(southWest, northEast, adminLevel, cancellationToken);

        var features = rawFeatures
            .SelectMany(feature =>
            {
                var stored = new StoredFeature(feature, Kind, zoom);
                // Namespace id by adminLevel so different levels don't collide on the same OSM id.
                stored.Id = $"{Kind}:{adminLevel}:{stored.FeatureId}";
                stored.Properties["adminLevel"] = adminLevel.ToString();
                var documents = new List<StoredFeature> { stored };

                if (feature.Geometry is not BAMCIS.GeoJSON.Point && (stored.X != x || stored.Y != y))
                {
                    documents.Add(StoredFeature.CreatePointer(
                        Kind,
                        stored.FeatureId ?? stored.LogicalId,
                        x,
                        y,
                        zoom,
                        stored.X,
                        stored.Y,
                        stored.Zoom,
                        stored.Id,
                        new Dictionary<string, dynamic>
                        {
                            ["adminLevel"] = adminLevel.ToString()
                        }));
                }

                return documents;
            })
            .ToList();

        if (features.Count == 0)
        {
            var empty = new StoredFeature(Kind, x, y, zoom)
            {
                Id = $"empty-{Kind}-{adminLevel}-{zoom}-{x}-{y}",
            };
            empty.Properties["adminLevel"] = adminLevel.ToString();
            features.Add(empty);
        }

        await UpsertBoundaryDocuments(features, cancellationToken);
        return features;
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

        var currentDocument = document;
        foreach (var step in new[] { 1, 2, 4, 8, 16, 32 })
        {
            try
            {
                if (step > 1)
                    currentDocument = CreateSimplifiedBoundaryDocument(document, step);

                await UpsertDocument(currentDocument, cancellationToken);
                return;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.RequestEntityTooLarge && step < 32)
            {
                continue;
            }
        }

        await UpsertDocument(CreateSimplifiedBoundaryDocument(document, 64), cancellationToken);
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
}
