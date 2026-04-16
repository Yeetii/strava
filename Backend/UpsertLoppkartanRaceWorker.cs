using Azure.Messaging.ServiceBus;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class UpsertLoppkartanRaceWorker(
    IHttpClientFactory httpClientFactory,
    RaceCollectionClient racesCollectionClient,
    ILogger<UpsertLoppkartanRaceWorker> logger)
{
    private const int Zoom = RaceCollectionClient.DefaultZoom;
    private static readonly Uri SourceUrl = new("https://www.loppkartan.se/markers-se.json");

    [Function(nameof(UpsertLoppkartanRaceWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.UpsertLoppkartanRace, Connection = "ServicebusConnection")] ServiceBusReceivedMessage message,
        CancellationToken cancellationToken)
    {
        LoppkartanScrapeTarget? target;
        try
        {
            target = message.Body.ToObjectFromJson<LoppkartanScrapeTarget>();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to deserialize Loppkartan race message");
            return;
        }

        if (target is null || string.IsNullOrWhiteSpace(target.MarkerId))
            return;

        try
        {
            var baseName = target.Name ?? target.Location ?? $"Loppkartan {target.MarkerId}";

            var properties = new Dictionary<string, dynamic>
            {
                [RaceScrapeDiscovery.PropName] = baseName,
                ["sourceUrl"] = SourceUrl.AbsoluteUri,
                [RaceScrapeDiscovery.LastScrapedUtcProperty] = DateTime.UtcNow.ToString("o")
            };

            if (!string.IsNullOrWhiteSpace(target.Website))
                properties[RaceScrapeDiscovery.PropWebsite] = target.Website;
            if (!string.IsNullOrWhiteSpace(target.Location))
                properties[RaceScrapeDiscovery.PropLocation] = target.Location;
            if (!string.IsNullOrWhiteSpace(target.County))
                properties["county"] = target.County;
            var normalizedDate = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(target.RaceDate);
            if (!string.IsNullOrWhiteSpace(normalizedDate))
                properties[RaceScrapeDiscovery.PropDate] = normalizedDate;
            var normalizedRaceType = RaceScrapeDiscovery.NormalizeRaceType(target.RaceType);
            if (!string.IsNullOrWhiteSpace(normalizedRaceType))
                properties[RaceScrapeDiscovery.PropRaceType] = normalizedRaceType;
            if (!string.IsNullOrWhiteSpace(target.TypeLocal))
                properties["typeLocal"] = target.TypeLocal;
            if (!string.IsNullOrWhiteSpace(target.DomainName))
                properties["domainName"] = target.DomainName;
            var normalizedCountry = RaceScrapeDiscovery.NormalizeCountryToIso2(target.OriginCountry);
            if (!string.IsNullOrWhiteSpace(normalizedCountry))
                properties[RaceScrapeDiscovery.PropCountry] = normalizedCountry;
            var normalizedDistance = RaceScrapeDiscovery.ParseDistanceVerbose(target.DistanceVerbose);
            if (!string.IsNullOrWhiteSpace(normalizedDistance))
                properties[RaceScrapeDiscovery.PropDistance] = normalizedDistance;

            var baseFeatureId = $"loppkartan:{target.MarkerId}";

            // Attempt to enrich with GPX routes from the event website.
            if (!string.IsNullOrWhiteSpace(target.Website) &&
                Uri.TryCreate(target.Website, UriKind.Absolute, out var websiteUri) &&
                websiteUri.Scheme is "http" or "https")
            {
                var routes = await TryFetchWebsiteGpxRoutes(httpClientFactory.CreateClient(), websiteUri, cancellationToken);
                if (routes.Count > 0)
                {
                    var routeDistancesKm = routes.Select(r => GpxParser.CalculateDistanceKm(r.Route.Coordinates)).ToList();
                    var distanceAssignments = RaceScrapeDiscovery.AssignDistancesToRoutes(routeDistancesKm, target.DistanceVerbose);

                    for (int i = 0; i < routes.Count; i++)
                    {
                        var (route, gpxUrl) = routes[i];
                        var gpxDistanceKm = routeDistancesKm[i];
                        var assignedDistances = distanceAssignments[i];

                        var routeProps = new Dictionary<string, dynamic>(properties);
                        routeProps["gpxUrl"] = gpxUrl.AbsoluteUri;

                        // Naming: when multiple routes use "{baseName} {primaryDistance}";
                        // when single route keep GPX internal name (if set) or base name.
                        if (routes.Count > 1)
                        {
                            var primaryDistance = assignedDistances.Count > 0
                                ? assignedDistances[0]
                                : RaceScrapeDiscovery.FormatDistanceKm(gpxDistanceKm);
                            routeProps[RaceScrapeDiscovery.PropName] = $"{baseName} {primaryDistance}";
                        }
                        else if (!string.IsNullOrWhiteSpace(route.Name))
                        {
                            routeProps[RaceScrapeDiscovery.PropName] = route.Name;
                        }

                        // Distance: all assigned verbose distances, or computed distance as fallback.
                        if (assignedDistances.Count > 0)
                            routeProps[RaceScrapeDiscovery.PropDistance] = string.Join(", ", assignedDistances);
                        else if (gpxDistanceKm > 0)
                            routeProps[RaceScrapeDiscovery.PropDistance] = RaceScrapeDiscovery.FormatDistanceKm(gpxDistanceKm);

                        var routeFeatureId = routes.Count == 1 ? baseFeatureId : $"{baseFeatureId}:{i}";
                        var lineString = new LineString(route.Coordinates.Select(c => new Position(c.Lng, c.Lat)).ToList());
                        var feature = new Feature(lineString, routeProps, null, new FeatureId(routeFeatureId));
                        var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
                        await racesCollectionClient.UpsertDocument(stored, cancellationToken);
                        logger.LogInformation("Loppkartan: upserted GPX route {RouteFeatureId} from {GpxUrl}", routeFeatureId, gpxUrl);
                    }

                    return;
                }
            }

            // Fallback: store the scraped point position.
            var point = new Point(new Position(target.Longitude, target.Latitude));
            var pointFeature = new Feature(point, properties, null, new FeatureId(baseFeatureId));
            var pointStored = new StoredFeature(pointFeature, FeatureKinds.Race, Zoom);
            await racesCollectionClient.UpsertDocument(pointStored, cancellationToken);
            logger.LogInformation("Loppkartan: upserted marker {MarkerId}", target.MarkerId);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Failed to upsert Loppkartan marker {MarkerId}", target.MarkerId);
        }
    }

    private async Task<IReadOnlyList<(ParsedGpxRoute Route, Uri GpxUrl)>> TryFetchWebsiteGpxRoutes(
        HttpClient httpClient,
        Uri websiteUri,
        CancellationToken cancellationToken)
    {
        // BFS: start from the race website and follow course-related links up to MaxDepth levels.
        // Collect GPX links from every visited page, then resolve each to a parsed route.
        const int MaxDepth = 3;
        const int MaxPages = 30;

        var visitedPages = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var gpxLinkUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pageQueue = new Queue<(Uri PageUri, int Depth)>();
        pageQueue.Enqueue((websiteUri, 0));

        while (pageQueue.Count > 0 && visitedPages.Count < MaxPages)
        {
            var (pageUri, depth) = pageQueue.Dequeue();
            if (!visitedPages.Add(pageUri.AbsoluteUri)) continue;

            var html = await TryFetchString(httpClient, pageUri, cancellationToken);
            if (html is null) continue;

            foreach (var gpxLink in RaceScrapeDiscovery.ExtractGpxLinksFromHtml(html, pageUri))
                gpxLinkUrls.Add(gpxLink.AbsoluteUri);

            if (depth < MaxDepth)
            {
                foreach (var courseLink in RaceScrapeDiscovery.ExtractCourseLinksFromHtml(html, pageUri))
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
            var result = await TryFetchGpxFromUrl(httpClient, gpxUri, triedUrls, cancellationToken);
            if (result.HasValue)
                routes.Add(result.Value);
        }

        return routes;
    }

    // Attempts to resolve a URL to a parsed GPX route.
    //
    // 1. Fetch the URL — if the response is a valid GPX file, parse and return it.
    // 2. If the response is HTML, look for GPX links on that page and try to download each.
    // 3. If still not found, look for generic download links and try those.
    private async Task<(ParsedGpxRoute Route, Uri GpxUrl)?> TryFetchGpxFromUrl(
        HttpClient httpClient,
        Uri url,
        HashSet<string> triedUrls,
        CancellationToken cancellationToken)
    {
        if (!triedUrls.Add(url.AbsoluteUri)) return null;

        var content = await TryFetchString(httpClient, url, cancellationToken);
        if (content is null) return null;

        // Try to parse as GPX directly.
        var parsed = GpxParser.TryParseRoute(content);
        if (parsed is not null)
            return (parsed, url);

        // The response is likely HTML — look for GPX links on the page.
        foreach (var gpxLink in RaceScrapeDiscovery.ExtractGpxLinksFromHtml(content, url))
        {
            if (triedUrls.Contains(gpxLink.AbsoluteUri)) continue;
            triedUrls.Add(gpxLink.AbsoluteUri);

            var gpxContent = await TryFetchString(httpClient, gpxLink, cancellationToken);
            if (gpxContent is null) continue;

            var gpxParsed = GpxParser.TryParseRoute(gpxContent);
            if (gpxParsed is not null)
                return (gpxParsed, gpxLink);
        }

        // No GPX found via GPX links — try generic download links.
        foreach (var dlLink in RaceScrapeDiscovery.ExtractDownloadLinksFromHtml(content, url))
        {
            if (triedUrls.Contains(dlLink.AbsoluteUri)) continue;
            triedUrls.Add(dlLink.AbsoluteUri);

            var dlContent = await TryFetchString(httpClient, dlLink, cancellationToken);
            if (dlContent is null) continue;

            var dlParsed = GpxParser.TryParseRoute(dlContent);
            if (dlParsed is not null)
                return (dlParsed, dlLink);
        }

        return null;
    }

    private async Task<string?> TryFetchString(HttpClient httpClient, Uri url, CancellationToken cancellationToken)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));
            return await httpClient.GetStringAsync(url, cts.Token);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug(ex, "Loppkartan: could not fetch {Url}", url);
            return null;
        }
    }
}
