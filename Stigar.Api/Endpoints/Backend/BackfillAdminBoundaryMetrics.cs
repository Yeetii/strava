using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using Microsoft.Extensions.Logging;
using Stigar.Core.Services;
using Stigar.Core.Jobs;
using Stigar.Core.Services.Postgres.Admin;
using Stigar.Core.Services.Postgres.Spatial;

namespace Stigar.Api.Endpoints.Backend;

public class BackfillAdminBoundaryMetrics(
    AdminBoundaryMetricsEnricher enricher,
    IAdminBoundaryRepository adminBoundaries,
    IBackendAdminQueries queries,
    IJobEpochService epochs,
    ILogger<BackfillAdminBoundaryMetrics> logger)
{
    private const int DefaultBatchSize = 5;
    private const int MaxBatchSize = 25;
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/boundaries/backfill", (HttpContext context, [FromServices] BackfillAdminBoundaryMetrics endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        var batchSize = ParseBatchSize(context);

        var epoch = await epochs.GetAsync(JobEpochType.AdminBoundaryMetrics, cancellationToken);
        var boundaryIds = await queries.GetStaleAdminBoundaryIds(
            epoch,
            afterId: null,
            batchSize,
            cancellationToken);
        var boundaries = await adminBoundaries.GetByIds(boundaryIds, cancellationToken);

        var processed = 0;
        var failed = new List<string>();

        foreach (var boundary in boundaries)
        {
            try
            {
                logger.LogInformation("Backfilling admin boundary {BoundaryId}", boundary.Id);
                var metrics = await enricher.CalculateMetricsAsync(boundary, cancellationToken);
                await adminBoundaries.UpdateMetrics(boundary.Id, metrics, epoch, cancellationToken);
                processed++;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to backfill admin boundary {BoundaryId}", boundary.Id);
                failed.Add(boundary.Id);
            }
        }

        return TypedResults.Ok(new
        {
            requestedBatchSize = batchSize,
            fetched = boundaryIds.Count,
            processed,
            failed,
            hasMore = boundaryIds.Count == batchSize
        });
    }

    private static int ParseBatchSize(HttpContext req)
    {
        if (!int.TryParse(req.Request.Query["batchSize"], out var batchSize))
        {
            return DefaultBatchSize;
        }

        return Math.Clamp(batchSize, 1, MaxBatchSize);
    }
}
