using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Spatial;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Geo;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public class MoveOsmFeaturesByZoom(
    CosmosClient cosmosClient,
    CollectionClient<StoredFeature> storedFeaturesCollection,
    IConfiguration configuration,
    ILogger<MoveOsmFeaturesByZoom> logger)
{
    private static readonly HashSet<string> ValidKinds = new(StringComparer.OrdinalIgnoreCase)
    {
        FeatureKinds.Peak,
        FeatureKinds.Path,
        FeatureKinds.ProtectedArea,
        FeatureKinds.AdminBoundary,
        FeatureKinds.Race,
    };

    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "kind", In = ParameterLocation.Query, Type = typeof(string), Required = true,
        Description = "Feature kind to move: peak | path | protectedArea | adminBoundary | race")]
    [OpenApiParameter(name: "fromZoom", In = ParameterLocation.Query, Type = typeof(int), Required = true,
        Description = "Source zoom level for documents to move.")]
    [OpenApiParameter(name: "toZoom", In = ParameterLocation.Query, Type = typeof(int), Required = true,
        Description = "Destination zoom level for documents to move.")]
    [OpenApiParameter(name: "ids", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional comma-separated feature IDs to move. IDs may be provided with or without the kind prefix.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(MoveZoomResult),
        Description = "Summary of moved documents.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(MoveOsmFeaturesByZoom))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/osmfeatures/moveZoom")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var kind = req.Query["kind"];
        if (string.IsNullOrWhiteSpace(kind))
            return await CreateBadRequest(req, "Query parameter 'kind' is required.");

        if (!ValidKinds.Contains(kind))
            return await CreateBadRequest(req, $"Unknown kind '{kind}'. Valid values: {string.Join(", ", ValidKinds)}");

        var fromZoomParam = req.Query["fromZoom"];
        var toZoomParam = req.Query["toZoom"];
        if (!int.TryParse(fromZoomParam, out var fromZoom))
            return await CreateBadRequest(req, "Query parameter 'fromZoom' is required and must be an integer.");
        if (!int.TryParse(toZoomParam, out var toZoom))
            return await CreateBadRequest(req, "Query parameter 'toZoom' is required and must be an integer.");
        if (fromZoom == toZoom)
            return await CreateBadRequest(req, "Query parameters 'fromZoom' and 'toZoom' must differ.");

        var ids = ParseIds(req.Query["ids"], kind);
        var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer);

        var sourceQueryText = "SELECT * FROM c WHERE c.kind = @kind AND c.zoom = @fromZoom";
        var sourceQueryDefinition = new QueryDefinition(sourceQueryText)
            .WithParameter("@kind", kind)
            .WithParameter("@fromZoom", fromZoom);

        if (ids.Count > 0)
        {
            var inClause = string.Join(",", ids.Select((_, i) => $"@id{i}"));
            sourceQueryDefinition = new QueryDefinition(sourceQueryText + $" AND c.id IN ({inClause})")
                .WithParameter("@kind", kind)
                .WithParameter("@fromZoom", fromZoom);

            for (int i = 0; i < ids.Count; i++)
            {
                sourceQueryDefinition = sourceQueryDefinition.WithParameter($"@id{i}", ids.ElementAt(i));
            }
        }

        const int MaxConcurrentDocumentMoves = 4;
        var totalScanned = 0;
        var totalMoved = 0;
        var totalSkipped = 0;
        var pageNumber = 0;

        using var feedIterator = container.GetItemQueryIterator<StoredFeature>(sourceQueryDefinition);
        while (feedIterator.HasMoreResults)
        {
            pageNumber++;
            var page = await feedIterator.ReadNextAsync(cancellationToken);
            var documentsToMove = page
                .Where(document => !StoredFeature.IsPointerDocument(document)
                    && !document.Id.StartsWith($"empty-{kind}-", StringComparison.Ordinal))
                .ToList();

            totalScanned += page.Count;
            totalSkipped += page.Count - documentsToMove.Count;
            var pageMoveCount = documentsToMove.Count;

            if (pageMoveCount > 0)
            {
                logger.LogInformation(
                    "About to execute {PageMoveCount} document move operations for kind {Kind} from zoom {FromZoom} to zoom {ToZoom} in page {PageNumber}.",
                    pageMoveCount,
                    kind,
                    fromZoom,
                    toZoom,
                    pageNumber);

                var batchSize = 20;
                var batches = documentsToMove
                    .Select((document, index) => new { document, index })
                    .GroupBy(x => x.index / batchSize)
                    .Select(group => group.Select(x => x.document).ToList())
                    .ToList();

                var batchNumber = 0;
                foreach (var batch in batches)
                {
                    batchNumber++;
                    logger.LogInformation(
                        "Processing batch {BatchNumber}/{BatchCount} with {BatchCountDocuments} documents for kind {Kind} from zoom {FromZoom} to zoom {ToZoom}.",
                        batchNumber,
                        batches.Count,
                        batch.Count,
                        kind,
                        fromZoom,
                        toZoom);

                    var movedDocuments = batch.Select(document =>
                    {
                        var centroid = GeometryCentroidHelper.GetCentroid(document.Geometry);
                        var (targetX, targetY) = SlippyTileCalculator.WGS84ToTileIndex(centroid, toZoom);
                        return new StoredFeature
                        {
                            Id = document.Id,
                            Kind = document.Kind,
                            FeatureId = document.FeatureId,
                            Geometry = document.Geometry,
                            Properties = document.Properties,
                            X = targetX,
                            Y = targetY,
                            Zoom = toZoom,
                        };
                    }).ToList();

                    await storedFeaturesCollection.BulkUpsert(movedDocuments, MaxConcurrentDocumentMoves, cancellationToken);

                    var patches = batch.Select(document =>
                    {
                        var oldPartitionKey = new PartitionKeyBuilder().Add(document.X).Add(document.Y).Build();
                        return (document.Id, oldPartitionKey, new[] { PatchOperation.Set("/ttl", 1) } as IReadOnlyList<PatchOperation>);
                    }).ToList();

                    await storedFeaturesCollection.PatchDocuments(patches, cancellationToken);

                    foreach (var document in batch)
                    {
                        var centroid = GeometryCentroidHelper.GetCentroid(document.Geometry);
                        var (targetX, targetY) = SlippyTileCalculator.WGS84ToTileIndex(centroid, toZoom);
                        logger.LogInformation(
                            "Moved document {DocumentId} from zoom {FromZoom} ({X},{Y}) to zoom {ToZoom} ({TargetX},{TargetY})",
                            document.Id, fromZoom, document.X, document.Y, toZoom, targetX, targetY);
                    }
                }

                totalMoved += pageMoveCount;
            }
        }

        logger.LogInformation(
            "Found {TotalScanned} source documents for kind {Kind} from zoom {FromZoom} to zoom {ToZoom}. Moved={TotalMoved}, Skipped={TotalSkipped}.",
            totalScanned,
            kind,
            fromZoom,
            toZoom,
            totalMoved,
            totalSkipped);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new MoveZoomResult(kind, fromZoom, toZoom, totalScanned, totalMoved, totalSkipped));
        return response;
    }

    private static HashSet<string> ParseIds(string? idsParam, string kind)
    {
        if (string.IsNullOrWhiteSpace(idsParam))
            return new HashSet<string>(StringComparer.Ordinal);

        return idsParam
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(id => StoredFeature.NormalizeFeatureId(kind, id))
            .Select(id => StoredFeature.EnsurePrefixedFeatureId(kind, id))
            .ToHashSet(StringComparer.Ordinal);
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
            return false;

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
    }

    private static async Task<HttpResponseData> CreateBadRequest(HttpRequestData req, string message)
    {
        var response = req.CreateResponse(HttpStatusCode.BadRequest);
        await response.WriteStringAsync(message);
        return response;
    }
}

public record MoveZoomResult(
    string Kind,
    int FromZoom,
    int ToZoom,
    int Scanned,
    int Moved,
    int Skipped);
