using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Shared.Services.Postgres.Admin;
using Shared.Services.Postgres.Spatial;

namespace Backend;

public class BackfillAdminBoundaryMetrics(
    AdminBoundaryMetricsEnricher enricher,
    IAdminBoundaryRepository adminBoundaries,
    IBackendAdminQueries queries,
    ILogger<BackfillAdminBoundaryMetrics> logger)
{
    private const int DefaultBatchSize = 5;
    private const int MaxBatchSize = 25;

    [Function(nameof(BackfillAdminBoundaryMetrics))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "boundaries/backfill")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        var batchSize = ParseBatchSize(req);

        // Same selection as the old Cosmos query: non-empty admin boundaries whose metrics
        // version is behind the current one, ordered by id for deterministic paging.
        var boundaryIds = await queries.GetStaleAdminBoundaryIds(
            AdminBoundaryMetricsEnricher.MetricsVersion,
            afterId: null,
            batchSize,
            cancellationToken);
        var boundaries = await adminBoundaries.GetByIds(boundaryIds, cancellationToken);

        var processed = 0;
        var failed = new List<string>();

        foreach (var boundary in boundaries.Where(OsmFeaturesChangeTrigger.ShouldEnrich))
        {
            try
            {
                logger.LogInformation("Backfilling admin boundary {BoundaryId}", boundary.Id);
                var metrics = await enricher.CalculateMetricsAsync(boundary, cancellationToken);
                await adminBoundaries.UpdateMetrics(
                    boundary.Id, metrics, AdminBoundaryMetricsEnricher.MetricsVersion, cancellationToken);
                processed++;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to backfill admin boundary {BoundaryId}", boundary.Id);
                failed.Add(boundary.Id);
            }
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            requestedBatchSize = batchSize,
            fetched = boundaryIds.Count,
            processed,
            failed,
            hasMore = boundaryIds.Count == batchSize
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
