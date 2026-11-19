using Microsoft.Extensions.Logging;
using Shared.Jobs;
using Shared.Models;
using Shared.Services;

namespace Backend;

/// <summary>
/// Native Hangfire consumer for one race scrape request. The scraping logic
/// lives in the shared RaceScrapePipeline so the API's manual endpoint and this
/// worker execute exactly the same code.
/// </summary>
public sealed class ScrapeRaceWorker(
    IOrganizerStore organizerClient,
    RaceScrapePipeline pipeline,
    ILogger<ScrapeRaceWorker> logger) : IScrapeRaceJob
{
    public async Task ExecuteAsync(ScrapeRaceMessage request, CancellationToken cancellationToken)
    {
        var organizerKey = request.OrganizerKey.Trim();
        if (string.IsNullOrWhiteSpace(organizerKey))
        {
            logger.LogWarning("Empty organizer key in scrape job");
            return;
        }

        var doc = await organizerClient.GetByIdAsync(organizerKey, cancellationToken);
        if (doc is null)
        {
            logger.LogWarning("Organizer document not found: {Key}", organizerKey);
            return;
        }

        if (!request.IsUrgent && RaceScrapePipeline.IsFreshEnoughForAutomaticScrape(doc, DateTime.UtcNow))
        {
            logger.LogInformation("Skipping automatic scrape for {Key}; organizer was scraped recently at {LastScrapedUtc}", organizerKey, doc.LastScrapedUtc);
            return;
        }

        await pipeline.RunAsync(organizerKey, doc, cancellationToken);
    }
}
