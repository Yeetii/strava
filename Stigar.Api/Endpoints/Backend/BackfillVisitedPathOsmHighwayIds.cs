using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using Hangfire;
using Stigar.Core.Services;
using System.Net;
using Microsoft.Extensions.Logging;
using Stigar.Core.Constants;
using Stigar.Core.Services.Postgres.Admin;

namespace Stigar.Api.Endpoints.Backend;

public class BackfillVisitedPathOsmHighwayIds(
    IBackendAdminQueries queries,
    IBackgroundJobClient jobs,
    ILogger<BackfillVisitedPathOsmHighwayIds> logger)
{
    private const int DefaultBatchSize = 200;
    private const int MaxBatchSize = 1000;
    private const int DefaultMaxPages = 1000;
    private const int MaxMaxPages = 10000;
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/visited-paths/backfill-osm-highway-ids", (HttpContext context, [FromServices] BackfillVisitedPathOsmHighwayIds endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        var batchSize = ParseBatchSize(context);
        var continuationToken = context.Request.Query["continuationToken"].ToString();

        var (fetchedDocuments, queuedActivities, nextContinuationToken) = await ProcessPageAsync(
            batchSize,
            continuationToken,
            cancellationToken);

        logger.LogInformation(
            "Visited-path OSM backfill queued {QueuedCount} activity messages from {DocumentCount} documents (batchSize={BatchSize}, hasMore={HasMore})",
            queuedActivities,
            fetchedDocuments,
            batchSize,
            !string.IsNullOrWhiteSpace(nextContinuationToken));

        return TypedResults.Ok(new
        {
            requestedBatchSize = batchSize,
            fetchedDocuments,
            queuedActivities,
            hasMore = !string.IsNullOrWhiteSpace(nextContinuationToken),
            continuationToken = nextContinuationToken
        });
    }
    public static RouteHandlerBuilder MapAll(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/visited-paths/backfill-osm-highway-ids/all", (HttpContext context, [FromServices] BackfillVisitedPathOsmHighwayIds endpoint) => endpoint.HandleAll(context, context.RequestAborted));

    public async Task<IResult> HandleAll(HttpContext context, CancellationToken cancellationToken)
    {
        var batchSize = ParseBatchSize(context);
        var maxPages = ParseMaxPages(context);
        var pageCount = 0;
        var totalDocuments = 0;
        var totalQueued = 0;
        string? continuationToken = context.Request.Query["continuationToken"].ToString();

        do
        {
            pageCount++;
            var (fetchedDocuments, queuedActivities, nextContinuationToken) = await ProcessPageAsync(
                batchSize,
                continuationToken,
                cancellationToken);

            totalDocuments += fetchedDocuments;
            totalQueued += queuedActivities;
            continuationToken = nextContinuationToken;
        }
        while (!string.IsNullOrWhiteSpace(continuationToken) && pageCount < maxPages);

        var stoppedByLimit = !string.IsNullOrWhiteSpace(continuationToken) && pageCount >= maxPages;
        logger.LogInformation(
            "Visited-path OSM full backfill processed {PageCount} pages, {DocumentCount} documents, queued {QueuedCount} activity messages. StoppedByLimit={StoppedByLimit}",
            pageCount,
            totalDocuments,
            totalQueued,
            stoppedByLimit);

        return TypedResults.Ok(new
        {
            requestedBatchSize = batchSize,
            maxPages,
            processedPages = pageCount,
            fetchedDocuments = totalDocuments,
            queuedActivities = totalQueued,
            completed = string.IsNullOrWhiteSpace(continuationToken),
            stoppedByLimit,
            continuationToken
        });
    }

    private async Task<(int FetchedDocuments, int QueuedActivities, string? ContinuationToken)> ProcessPageAsync(
        int batchSize,
        string? continuationToken,
        CancellationToken cancellationToken)
    {
        // Deterministic keyset paging over activities lacking visited tiles instead of the old Cosmos
        // continuation token. The token is the last (highest) activity_id from the page.
        var activityIds = await queries.GetActivityIdsWithMissingOsmHighwayId(
            continuationToken,
            batchSize,
            cancellationToken);

        var queuedActivities = await QueueActivityCollectionJobs.QueueActivityIdsAsync(
            activityIds,
            jobs,
            QueueConfig.CalculateVisitedPathsJobs,
            cancellationToken);

        var nextContinuationToken = activityIds.Count == batchSize ? activityIds[^1] : null;
        return (activityIds.Count, queuedActivities, nextContinuationToken);
    }

    private static int ParseBatchSize(HttpContext req)
    {
        if (!int.TryParse(req.Request.Query["batchSize"], out var batchSize))
        {
            return DefaultBatchSize;
        }

        return Math.Clamp(batchSize, 1, MaxBatchSize);
    }

    private static int ParseMaxPages(HttpContext req)
    {
        if (!int.TryParse(req.Request.Query["maxPages"], out var maxPages))
        {
            return DefaultMaxPages;
        }

        return Math.Clamp(maxPages, 1, MaxMaxPages);
    }
}
