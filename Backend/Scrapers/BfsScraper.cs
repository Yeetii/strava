using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend.Scrapers;

// Performs a breadth-first search from a race website URL to discover GPX route files.
// Depth 3 / max 30 pages.  Used directly for Loppkartan websites and as a helper
// by TraceDeTrailEventScraper after the "Site de la course" URL is extracted.
internal sealed class BfsScraper(ILogger logger) : IRaceScraper
{
    private const int MaxDepth = 3;
    private const int MaxPages = 30;

    public bool CanHandle(ScrapeJob job) => job.WebsiteUrl is not null;

    public async Task<RaceScraperResult?> ScrapeAsync(ScrapeJob job, HttpClient httpClient, CancellationToken cancellationToken)
    {
        if (job.WebsiteUrl is null) return null;
        return await ScrapeFromUrlAsync(job, job.WebsiteUrl, httpClient, cancellationToken);
    }

    // Runs BFS from an explicit URL.  Called by TraceDeTrailEventScraper as well.
    public async Task<RaceScraperResult?> ScrapeFromUrlAsync(ScrapeJob job, Uri startUrl, HttpClient httpClient, CancellationToken cancellationToken)
    {
        var rawRoutes = await FindGpxRoutesAsync(httpClient, startUrl, cancellationToken);
        if (rawRoutes.Count == 0)
            return null;

        var baseName = job.Name ?? job.Location ?? "Unnamed";
        var routeDistancesKm = rawRoutes.Select(r => GpxParser.CalculateDistanceKm(r.Route.Coordinates)).ToList();
        var distanceAssignments = RaceScrapeDiscovery.AssignDistancesToRoutes(routeDistancesKm, job.Distance);

        var routes = new List<ScrapedRoute>(rawRoutes.Count);
        for (int i = 0; i < rawRoutes.Count; i++)
        {
            var (parsedRoute, gpxUrl) = rawRoutes[i];
            var gpxDistanceKm = routeDistancesKm[i];
            var assignedDistances = distanceAssignments[i];

            string? routeName = null;
            if (rawRoutes.Count > 1)
            {
                var primaryDistance = assignedDistances.Count > 0
                    ? assignedDistances[0]
                    : RaceScrapeDiscovery.FormatDistanceKm(gpxDistanceKm);
                routeName = $"{baseName} {primaryDistance}";
            }
            else if (!string.IsNullOrWhiteSpace(parsedRoute.Name))
            {
                routeName = parsedRoute.Name;
            }

            var routeDistance = assignedDistances.Count > 0
                ? string.Join(", ", assignedDistances)
                : gpxDistanceKm > 0 ? RaceScrapeDiscovery.FormatDistanceKm(gpxDistanceKm) : null;

            routes.Add(new ScrapedRoute(
                Coordinates: parsedRoute.Coordinates,
                SourceUrl: startUrl,
                Name: routeName,
                Distance: routeDistance,
                GpxUrl: gpxUrl));
        }

        return new RaceScraperResult(routes);
    }

    // BFS traversal: visits pages, collects GPX links, returns parsed routes.
    private async Task<IReadOnlyList<(ParsedGpxRoute Route, Uri GpxUrl)>> FindGpxRoutesAsync(
        HttpClient httpClient,
        Uri startUrl,
        CancellationToken cancellationToken)
    {
        var visitedPages = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var gpxLinkUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pageQueue = new Queue<(Uri PageUri, int Depth)>();
        pageQueue.Enqueue((startUrl, 0));

        while (pageQueue.Count > 0 && visitedPages.Count < MaxPages)
        {
            var (pageUri, depth) = pageQueue.Dequeue();
            if (!visitedPages.Add(pageUri.AbsoluteUri)) continue;

            var html = await TryFetchStringAsync(httpClient, pageUri, cancellationToken);
            if (html is null) continue;

            foreach (var gpxLink in RaceHtmlScraper.ExtractGpxLinksFromHtml(html, pageUri))
                gpxLinkUrls.Add(gpxLink.AbsoluteUri);

            if (depth < MaxDepth)
            {
                foreach (var courseLink in RaceHtmlScraper.ExtractCourseLinksFromHtml(html, pageUri))
                {
                    if (!visitedPages.Contains(courseLink.AbsoluteUri))
                        pageQueue.Enqueue((courseLink, depth + 1));
                }
            }
        }

        if (gpxLinkUrls.Count == 0)
            return [];

        var triedUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var routes = new List<(ParsedGpxRoute, Uri)>();

        foreach (var gpxUrlStr in gpxLinkUrls)
        {
            var gpxUri = new Uri(gpxUrlStr);
            var result = await TryFetchGpxFromUrlAsync(httpClient, gpxUri, triedUrls, cancellationToken);
            if (result.HasValue)
                routes.Add(result.Value);
        }

        return routes;
    }

    private async Task<(ParsedGpxRoute Route, Uri GpxUrl)?> TryFetchGpxFromUrlAsync(
        HttpClient httpClient,
        Uri url,
        HashSet<string> triedUrls,
        CancellationToken cancellationToken)
    {
        if (!triedUrls.Add(url.AbsoluteUri)) return null;

        var content = await TryFetchStringAsync(httpClient, url, cancellationToken);
        if (content is null) return null;

        var parsed = GpxParser.TryParseRoute(content);
        if (parsed is not null)
            return (parsed, url);

        foreach (var gpxLink in RaceHtmlScraper.ExtractGpxLinksFromHtml(content, url))
        {
            if (triedUrls.Contains(gpxLink.AbsoluteUri)) continue;
            triedUrls.Add(gpxLink.AbsoluteUri);

            var gpxContent = await TryFetchStringAsync(httpClient, gpxLink, cancellationToken);
            if (gpxContent is null) continue;

            var gpxParsed = GpxParser.TryParseRoute(gpxContent);
            if (gpxParsed is not null)
                return (gpxParsed, gpxLink);
        }

        foreach (var dlLink in RaceHtmlScraper.ExtractDownloadLinksFromHtml(content, url))
        {
            if (triedUrls.Contains(dlLink.AbsoluteUri)) continue;
            triedUrls.Add(dlLink.AbsoluteUri);

            var dlContent = await TryFetchStringAsync(httpClient, dlLink, cancellationToken);
            if (dlContent is null) continue;

            var dlParsed = GpxParser.TryParseRoute(dlContent);
            if (dlParsed is not null)
                return (dlParsed, dlLink);
        }

        return null;
    }

    private async Task<string?> TryFetchStringAsync(HttpClient httpClient, Uri url, CancellationToken cancellationToken)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));
            return await httpClient.GetStringAsync(url, cts.Token);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug(ex, "BfsScraper: could not fetch {Url}", url);
            return null;
        }
    }
}
