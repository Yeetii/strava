using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class BackfillAdminBoundaryMetrics(
    AdminBoundaryMetricsEnricher enricher,
    CollectionClient<StoredFeature> storedFeaturesCollection,
    ILogger<BackfillAdminBoundaryMetrics> logger)
{
    private const int DefaultBatchSize = 5;
    private const int MaxBatchSize = 25;

    private readonly AdminBoundaryMetricsEnricher _enricher = enricher;
    private readonly CollectionClient<StoredFeature> _storedFeaturesCollection = storedFeaturesCollection;
    private readonly ILogger<BackfillAdminBoundaryMetrics> _logger = logger;

    [Function(nameof(BackfillAdminBoundaryMetrics))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "boundaries/backfill")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        var batchSize = ParseBatchSize(req);
        var query = new QueryDefinition(@"
SELECT * FROM c
WHERE c.kind = @kind
AND NOT STARTSWITH(c.id, @emptyPrefix)
AND (
    NOT IS_DEFINED(c.properties.adminBoundaryMetricsVersion)
    OR c.properties.adminBoundaryMetricsVersion < @metricsVersion
)")
            .WithParameter("@kind", FeatureKinds.AdminBoundary)
            .WithParameter("@emptyPrefix", "empty-")
            .WithParameter("@metricsVersion", AdminBoundaryMetricsEnricher.MetricsVersion);
        var (boundaries, _) = await _storedFeaturesCollection.ExecuteQueryPageAsync<StoredFeature>(
            query,
            batchSize,
            cancellationToken: cancellationToken);

        var processed = 0;
        var failed = new List<string>();

        foreach (var boundary in boundaries.Where(EnrichNewAdminBoundaries.ShouldEnrich))
        {
            try
            {
                _logger.LogInformation("Backfilling admin boundary {BoundaryId}", boundary.Id);
                await _enricher.EnrichAsync(boundary, cancellationToken);
                await _storedFeaturesCollection.UpsertDocument(boundary, cancellationToken);
                processed++;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to backfill admin boundary {BoundaryId}", boundary.Id);
                failed.Add(boundary.Id);
            }
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            requestedBatchSize = batchSize,
            fetched = boundaries.Count,
            processed,
            failed,
            hasMore = boundaries.Count == batchSize
        }, cancellationToken);
        return response;
    }

    private static int ParseBatchSize(HttpRequestData req)
    {
        if (!int.TryParse(req.Query["batchSize"], out var batchSize))
        {
            return DefaultBatchSize;
        }

        return Math.Clamp(batchSize, 1, MaxBatchSize);
    }
}