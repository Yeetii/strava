using Microsoft.Extensions.Logging;
using Shared.Models;

namespace Backend.Scrapers;

// Fetches the ITRA JSON endpoint for a TraceDeTrail trace, converts Mercator points to WGS84,
// and returns the route as a single ScrapedRoute with elevation metadata.
internal sealed class ItraScraper(ILogger logger) : IRaceScraper
{
    public bool CanHandle(ScrapeJob job) => job.TraceDeTrailItraUrl is not null;

    public async Task<RaceScraperResult?> ScrapeAsync(ScrapeJob job, HttpClient httpClient, CancellationToken cancellationToken)
    {
        if (job.TraceDeTrailItraUrl is null) return null;

        string json;
        try
        {
            json = await httpClient.GetStringAsync(job.TraceDeTrailItraUrl, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "ITRA: failed to fetch {Url}", job.TraceDeTrailItraUrl);
            return null;
        }

        var traceData = RaceHtmlScraper.ParseTraceDeTrailTrace(json);

        if (traceData.Points.Count < 2)
        {
            logger.LogWarning("ITRA: trace {Url} returned fewer than 2 points, skipping", job.TraceDeTrailItraUrl);
            return null;
        }

        var distance = job.Distance ?? (traceData.TotalDistanceKm.HasValue
            ? RaceScrapeDiscovery.FormatDistanceKm(traceData.TotalDistanceKm.Value)
            : null);

        var coordinates = traceData.Points.Select(p => new Coordinate(p.Lng, p.Lat)).ToList();
        var route = new ScrapedRoute(
            Coordinates: coordinates,
            SourceUrl: job.TraceDeTrailItraUrl,
            Distance: distance,
            ElevationGain: traceData.ElevationGain);

        return new RaceScraperResult([route]);
    }
}
