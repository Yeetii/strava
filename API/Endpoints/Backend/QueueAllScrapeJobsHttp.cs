using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using Shared.Services;

namespace API.Endpoints.Backend;

public class QueueAllScrapeJobsHttp(
    IOrganizerStore raceOrganizerClient,
    RaceDiscoveryService discoveryService,
    ILogger<QueueAllScrapeJobsHttp> logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/queue-all-scrape-jobs",
            (HttpContext context, [FromServices] QueueAllScrapeJobsHttp endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        var allIds = await raceOrganizerClient.GetIdsDueForAutomaticScrapeAsync(DateTime.MaxValue, cancellationToken);
        logger.LogInformation("Manual HTTP trigger: queuing {Count} urgent scrape jobs for all organizers", allIds.Count);
        await discoveryService.EnqueueScrapeMessagesAsync(allIds.ToHashSet(StringComparer.OrdinalIgnoreCase), cancellationToken, isUrgent: true);
        return TypedResults.Ok($"Queued {allIds.Count} urgent scrape jobs");
    }
}
