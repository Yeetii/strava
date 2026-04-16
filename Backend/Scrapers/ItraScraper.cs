using Microsoft.Extensions.Logging;
using Shared.Models;

namespace Backend.Scrapers;

// Fetches the ITRA JSON endpoint for a TraceDeTrail trace, converts Mercator points to WGS84,
// and returns the route as a single ScrapedRoute with elevation metadata.
internal sealed class ItraScraper(ILogger logger) : IRaceScraper
{
    public bool CanHandle(ScrapeJob job) => job.TraceDeTrailItraUrls is { Count: > 0 };

    public async Task<RaceScraperResult?> ScrapeAsync(ScrapeJob job, HttpClient httpClient, CancellationToken cancellationToken)
    {
        if (job.TraceDeTrailItraUrls is not { Count: > 0 }) return null;

        var routes = new List<ScrapedRoute>();

        foreach (var itraUrl in job.TraceDeTrailItraUrls)
        {
            string json;
            try
            {
                json = await httpClient.GetStringAsync(itraUrl, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "ITRA: failed to fetch {Url}", itraUrl);
                continue;
            }

            var traceData = RaceHtmlScraper.ParseTraceDeTrailTrace(json);

            if (traceData.Points.Count < 2)
            {
                logger.LogWarning("ITRA: trace {Url} returned fewer than 2 points, skipping", itraUrl);
                continue;
            }

            var distance = traceData.TotalDistanceKm.HasValue
                ? RaceScrapeDiscovery.FormatDistanceKm(traceData.TotalDistanceKm.Value)
                : null;

            var coordinates = traceData.Points.Select(p => new Coordinate(p.Lng, p.Lat)).ToList();
            routes.Add(new ScrapedRoute(
                Coordinates: coordinates,
                SourceUrl: itraUrl,
                Distance: distance,
                ElevationGain: traceData.ElevationGain));
        }

        return routes.Count > 0 ? new RaceScraperResult(routes) : null;
    }
}
