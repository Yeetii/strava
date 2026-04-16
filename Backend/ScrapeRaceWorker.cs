using Azure.Messaging.ServiceBus;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class ScrapeRaceWorker(
    IHttpClientFactory httpClientFactory,
    ServiceBusClient serviceBusClient,
    RaceCollectionClient racesCollectionClient,
    ILogger<ScrapeRaceWorker> logger)
{
    private const int Zoom = RaceCollectionClient.DefaultZoom;

    // Limits for the BFS course-page traversal when searching for GPX routes.
    private const int GpxSearchMaxDepth = 3;
    private const int GpxSearchMaxPages = 30;

    private readonly ServiceBusSender _sender = serviceBusClient.CreateSender(ServiceBusConfig.ScrapeRace);

    [Function(nameof(ScrapeRaceWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.ScrapeRace, Connection = "ServicebusConnection")] ServiceBusReceivedMessage message,
        CancellationToken cancellationToken)
    {
        ScrapeJob? job;
        try
        {
            job = message.Body.ToObjectFromJson<ScrapeJob>();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to deserialize ScrapeJob message");
            return;
        }

        if (job is null) return;

        try
        {
            var url = job.Url;

            if (url is null)
            {
                await UpsertPointFallbackAsync(job, cancellationToken);
                return;
            }

            var host = url.Host;

            if (host.Equals("tracedetrail.fr", StringComparison.OrdinalIgnoreCase) &&
                url.AbsolutePath.StartsWith("/trace/getTraceItra/", StringComparison.OrdinalIgnoreCase))
            {
                await ScrapeItraAsync(job, url, cancellationToken);
                return;
            }

            if (host.Equals("tracedetrail.fr", StringComparison.OrdinalIgnoreCase) &&
                url.AbsolutePath.StartsWith("/en/event/", StringComparison.OrdinalIgnoreCase))
            {
                await ScrapeTraceDeTrailEventPageAsync(job, url, cancellationToken);
                return;
            }

            if (host.EndsWith(".utmb.world", StringComparison.OrdinalIgnoreCase) ||
                host.Equals("utmb.world", StringComparison.OrdinalIgnoreCase))
            {
                await ScrapeUtmbPageAsync(job, url, cancellationToken);
                return;
            }

            if (url.Scheme is "http" or "https")
            {
                await ScrapeGenericBfsAsync(job, url, cancellationToken);
                return;
            }

            logger.LogWarning("ScrapeRaceWorker: unhandled URL scheme for {Url}", url);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "ScrapeRaceWorker: failed to process job {Url}", job.Url);
        }
    }

    // ── ITRA scraper ─────────────────────────────────────────────────────────
    // Fetches the ITRA JSON endpoint, converts Mercator points to WGS84, and upserts as LineString.
    private async Task ScrapeItraAsync(ScrapeJob job, Uri url, CancellationToken cancellationToken)
    {
        string json;
        try
        {
            json = await httpClientFactory.CreateClient().GetStringAsync(url, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "ITRA: failed to fetch {Url}", url);
            return;
        }

        var traceData = RaceScrapeDiscovery.ParseTraceDeTrailTrace(json);

        if (traceData.Points.Count < 2)
        {
            logger.LogWarning("ITRA: trace {Url} returned fewer than 2 points, skipping", url);
            return;
        }

        var featureId = RaceScrapeDiscovery.BuildFeatureId(url);
        var lineString = new LineString(traceData.Points.Select(p => new Position(p.Lng, p.Lat)).ToList());

        var name = job.Name ?? url.Segments.LastOrDefault() ?? "Unnamed";
        var properties = new Dictionary<string, dynamic>
        {
            [RaceScrapeDiscovery.PropName] = name,
            [RaceScrapeDiscovery.PropWebsite] = url.AbsoluteUri,
            [RaceScrapeDiscovery.LastScrapedUtcProperty] = DateTime.UtcNow.ToString("o")
        };

        var distance = job.Distance ?? (traceData.TotalDistanceKm.HasValue
            ? RaceScrapeDiscovery.FormatDistanceKm(traceData.TotalDistanceKm.Value)
            : null);
        if (distance is not null)
            properties[RaceScrapeDiscovery.PropDistance] = distance;
        if (traceData.ElevationGain.HasValue)
            properties[RaceScrapeDiscovery.PropElevationGain] = traceData.ElevationGain.Value;

        var normalizedRaceType = RaceScrapeDiscovery.NormalizeRaceType(job.RaceType);
        if (!string.IsNullOrWhiteSpace(normalizedRaceType))
            properties[RaceScrapeDiscovery.PropRaceType] = normalizedRaceType;
        var normalizedCountry = RaceScrapeDiscovery.NormalizeCountryToIso2(job.Country);
        if (!string.IsNullOrWhiteSpace(normalizedCountry))
            properties[RaceScrapeDiscovery.PropCountry] = normalizedCountry;
        if (!string.IsNullOrWhiteSpace(job.ImageUrl))
            properties[RaceScrapeDiscovery.PropImage] = job.ImageUrl;

        var feature = new Feature(lineString, properties, null, new FeatureId(featureId));
        var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
        await racesCollectionClient.UpsertDocument(stored, cancellationToken);
        logger.LogInformation("ITRA: upserted trace {FeatureId}", featureId);
    }

    // ── TraceDeTrail event page scraper ───────────────────────────────────────
    // Fetches the event page, extracts "Site de la course", and re-enqueues as a BFS job.
    private async Task ScrapeTraceDeTrailEventPageAsync(ScrapeJob job, Uri url, CancellationToken cancellationToken)
    {
        string html;
        try
        {
            html = await httpClientFactory.CreateClient().GetStringAsync(url, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "TraceDeTrail event page: failed to fetch {Url}", url);
            return;
        }

        var siteUrl = RaceScrapeDiscovery.ExtractRaceSiteUrl(html, url);
        if (siteUrl is null)
        {
            logger.LogDebug("TraceDeTrail event page: no 'Site de la course' found on {Url}", url);
            return;
        }

        var newJob = job with { Url = siteUrl };
        var message = new ServiceBusMessage(BinaryData.FromObjectAsJson(newJob)) { ContentType = "application/json" };
        await _sender.SendMessageAsync(message, cancellationToken);
        logger.LogInformation("TraceDeTrail event page: re-enqueued BFS job for {SiteUrl}", siteUrl);
    }

    // ── UTMB scraper ──────────────────────────────────────────────────────────
    // Fetches the race HTML page, extracts GPX hrefs, downloads each file, and upserts.
    private async Task ScrapeUtmbPageAsync(ScrapeJob job, Uri url, CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();

        string html;
        try
        {
            html = await httpClient.GetStringAsync(url, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "UTMB: failed to fetch race page {Url}", url);
            return;
        }

        var gpxUrls = RaceScrapeDiscovery.ExtractGpxUrlsFromHtml(html, url)
            .GroupBy(u => u.AbsoluteUri, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .ToList();

        if (gpxUrls.Count == 0)
        {
            logger.LogWarning("UTMB: no GPX links found on race page {Url}", url);
            return;
        }

        for (int i = 0; i < gpxUrls.Count; i++)
            await TryUpsertGpxRouteAsync(httpClient, gpxUrls[i], job, gpxUrls.Count > 1 ? i : null, cancellationToken);
    }

    private async Task TryUpsertGpxRouteAsync(
        HttpClient httpClient,
        Uri gpxUrl,
        ScrapeJob job,
        int? routeIndex,
        CancellationToken cancellationToken)
    {
        if (job.Url is null) return;

        try
        {
            var gpxContent = await httpClient.GetStringAsync(gpxUrl, cancellationToken);
            var parsedRoute = GpxParser.TryParseRoute(gpxContent, job.Name ?? "Unnamed route");
            if (parsedRoute is null)
            {
                logger.LogWarning("UTMB: skipping GPX {GpxUrl}: failed to parse route points", gpxUrl);
                return;
            }

            var featureId = RaceScrapeDiscovery.BuildFeatureId(job.Url, routeIndex);
            var lineString = new LineString(parsedRoute.Coordinates.Select(c => new Position(c.Lng, c.Lat)).ToList());
            var properties = new Dictionary<string, dynamic>
            {
                [RaceScrapeDiscovery.PropName] = parsedRoute.Name,
                ["gpxUrl"] = gpxUrl.AbsoluteUri,
                ["gpx"] = gpxContent,
                [RaceScrapeDiscovery.PropWebsite] = job.Url.AbsoluteUri,
                [RaceScrapeDiscovery.PropRaceType] = "trail",
                [RaceScrapeDiscovery.LastScrapedUtcProperty] = DateTime.UtcNow.ToString("o")
            };

            if (!string.IsNullOrWhiteSpace(job.Distance))
                properties[RaceScrapeDiscovery.PropDistance] = job.Distance;
            if (job.ElevationGain.HasValue)
                properties[RaceScrapeDiscovery.PropElevationGain] = job.ElevationGain.Value;
            var normalizedCountry = RaceScrapeDiscovery.NormalizeCountryToIso2(job.Country);
            if (!string.IsNullOrWhiteSpace(normalizedCountry))
                properties[RaceScrapeDiscovery.PropCountry] = normalizedCountry;
            if (!string.IsNullOrWhiteSpace(job.Location))
                properties[RaceScrapeDiscovery.PropLocation] = job.Location;
            if (job.Playgrounds is { Count: > 0 })
                properties[RaceScrapeDiscovery.PropPlaygrounds] = job.Playgrounds;
            if (job.RunningStones is { Count: > 0 })
                properties[RaceScrapeDiscovery.PropRunningStones] = job.RunningStones;
            if (!string.IsNullOrWhiteSpace(job.ImageUrl))
                properties[RaceScrapeDiscovery.PropImage] = job.ImageUrl;

            var feature = new Feature(lineString, properties, null, new FeatureId(featureId));
            var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
            await racesCollectionClient.UpsertDocument(stored, cancellationToken);
            logger.LogInformation("UTMB: upserted race {FeatureId}", featureId);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "UTMB: failed to fetch or parse GPX from {GpxUrl}", gpxUrl);
        }
    }

    // ── Point fallback ────────────────────────────────────────────────────────
    // Stores a Point feature at the job's lat/lng when no URL or GPX is available.
    private async Task UpsertPointFallbackAsync(ScrapeJob job, CancellationToken cancellationToken)
    {
        if (job.Latitude is null || job.Longitude is null)
        {
            logger.LogWarning("ScrapeRaceWorker: point fallback skipped — no lat/lng");
            return;
        }

        var featureId = RaceScrapeDiscovery.BuildFeatureId(job.Name, job.Distance);
        if (string.IsNullOrEmpty(featureId))
        {
            logger.LogWarning("ScrapeRaceWorker: point fallback skipped — could not build feature ID");
            return;
        }

        var properties = BuildBaseProperties(job);
        var point = new Point(new Position(job.Longitude.Value, job.Latitude.Value));
        var feature = new Feature(point, properties, null, new FeatureId(featureId));
        var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
        await racesCollectionClient.UpsertDocument(stored, cancellationToken);
        logger.LogInformation("ScrapeRaceWorker: upserted point feature {FeatureId}", featureId);
    }

    // ── Generic BFS scraper ───────────────────────────────────────────────────
    // BFS from the race website: follows course links, collects GPX files, upserts each route.
    // Falls back to a point feature when no GPX routes are found.
    private async Task ScrapeGenericBfsAsync(ScrapeJob job, Uri startUrl, CancellationToken cancellationToken)
    {
        var baseName = job.Name ?? job.Location ?? "Unnamed";
        var httpClient = httpClientFactory.CreateClient();

        var routes = await TryFetchWebsiteGpxRoutes(httpClient, startUrl, cancellationToken);
        if (routes.Count > 0)
        {
            var routeDistancesKm = routes.Select(r => GpxParser.CalculateDistanceKm(r.Route.Coordinates)).ToList();
            var distanceAssignments = RaceScrapeDiscovery.AssignDistancesToRoutes(routeDistancesKm, job.Distance);

            for (int i = 0; i < routes.Count; i++)
            {
                var (route, gpxUrl) = routes[i];
                var gpxDistanceKm = routeDistancesKm[i];
                var assignedDistances = distanceAssignments[i];

                var routeProps = new Dictionary<string, dynamic>(BuildBaseProperties(job));
                routeProps["gpxUrl"] = gpxUrl.AbsoluteUri;

                // Naming: multiple routes use "{baseName} {primaryDistance}"; single route keeps GPX name.
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

                if (assignedDistances.Count > 0)
                    routeProps[RaceScrapeDiscovery.PropDistance] = string.Join(", ", assignedDistances);
                else if (gpxDistanceKm > 0)
                    routeProps[RaceScrapeDiscovery.PropDistance] = RaceScrapeDiscovery.FormatDistanceKm(gpxDistanceKm);

                var routeFeatureId = routes.Count == 1
                    ? RaceScrapeDiscovery.BuildFeatureId(startUrl)
                    : RaceScrapeDiscovery.BuildFeatureId(startUrl, i);
                var lineString = new LineString(route.Coordinates.Select(c => new Position(c.Lng, c.Lat)).ToList());
                var feature = new Feature(lineString, routeProps, null, new FeatureId(routeFeatureId));
                var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
                await racesCollectionClient.UpsertDocument(stored, cancellationToken);
                logger.LogInformation("ScrapeRaceWorker: upserted GPX route {FeatureId} from {GpxUrl}", routeFeatureId, gpxUrl);
            }

            return;
        }

        // No GPX found — fall back to point if we have coordinates.
        if (job.Latitude.HasValue && job.Longitude.HasValue)
        {
            var pointFeatureId = RaceScrapeDiscovery.BuildFeatureId(startUrl);
            var properties = BuildBaseProperties(job);
            var point = new Point(new Position(job.Longitude.Value, job.Latitude.Value));
            var pointFeature = new Feature(point, properties, null, new FeatureId(pointFeatureId));
            var pointStored = new StoredFeature(pointFeature, FeatureKinds.Race, Zoom);
            await racesCollectionClient.UpsertDocument(pointStored, cancellationToken);
            logger.LogInformation("ScrapeRaceWorker: upserted point feature {FeatureId}", pointFeatureId);
        }
        else
        {
            logger.LogDebug("ScrapeRaceWorker: no GPX routes and no coordinates for {Url}, skipping", startUrl);
        }
    }

    // Builds the base properties dictionary from a ScrapeJob (used by all scrapers).
    private static Dictionary<string, dynamic> BuildBaseProperties(ScrapeJob job)
    {
        var baseName = job.Name ?? job.Location ?? "Unnamed";
        var properties = new Dictionary<string, dynamic>
        {
            [RaceScrapeDiscovery.PropName] = baseName,
            [RaceScrapeDiscovery.LastScrapedUtcProperty] = DateTime.UtcNow.ToString("o")
        };

        if (job.Url is not null)
            properties[RaceScrapeDiscovery.PropWebsite] = job.Url.AbsoluteUri;
        if (!string.IsNullOrWhiteSpace(job.Location))
            properties[RaceScrapeDiscovery.PropLocation] = job.Location;
        if (!string.IsNullOrWhiteSpace(job.County))
            properties["county"] = job.County;
        var normalizedDate = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(job.Date);
        if (!string.IsNullOrWhiteSpace(normalizedDate))
            properties[RaceScrapeDiscovery.PropDate] = normalizedDate;
        var normalizedRaceType = RaceScrapeDiscovery.NormalizeRaceType(job.RaceType);
        if (!string.IsNullOrWhiteSpace(normalizedRaceType))
            properties[RaceScrapeDiscovery.PropRaceType] = normalizedRaceType;
        if (!string.IsNullOrWhiteSpace(job.TypeLocal))
            properties["typeLocal"] = job.TypeLocal;
        var normalizedCountry = RaceScrapeDiscovery.NormalizeCountryToIso2(job.Country);
        if (!string.IsNullOrWhiteSpace(normalizedCountry))
            properties[RaceScrapeDiscovery.PropCountry] = normalizedCountry;
        if (!string.IsNullOrWhiteSpace(job.Distance))
            properties[RaceScrapeDiscovery.PropDistance] = job.Distance;
        if (!string.IsNullOrWhiteSpace(job.ImageUrl))
            properties[RaceScrapeDiscovery.PropImage] = job.ImageUrl;

        return properties;
    }

    // ── BFS helpers (GPX discovery) ───────────────────────────────────────────

    private async Task<IReadOnlyList<(ParsedGpxRoute Route, Uri GpxUrl)>> TryFetchWebsiteGpxRoutes(
        HttpClient httpClient,
        Uri websiteUri,
        CancellationToken cancellationToken)
    {
        var visitedPages = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var gpxLinkUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pageQueue = new Queue<(Uri PageUri, int Depth)>();
        pageQueue.Enqueue((websiteUri, 0));

        while (pageQueue.Count > 0 && visitedPages.Count < GpxSearchMaxPages)
        {
            var (pageUri, depth) = pageQueue.Dequeue();
            if (!visitedPages.Add(pageUri.AbsoluteUri)) continue;

            var html = await TryFetchString(httpClient, pageUri, cancellationToken);
            if (html is null) continue;

            foreach (var gpxLink in RaceScrapeDiscovery.ExtractGpxLinksFromHtml(html, pageUri))
                gpxLinkUrls.Add(gpxLink.AbsoluteUri);

            if (depth < GpxSearchMaxDepth)
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

    private async Task<(ParsedGpxRoute Route, Uri GpxUrl)?> TryFetchGpxFromUrl(
        HttpClient httpClient,
        Uri url,
        HashSet<string> triedUrls,
        CancellationToken cancellationToken)
    {
        if (!triedUrls.Add(url.AbsoluteUri)) return null;

        var content = await TryFetchString(httpClient, url, cancellationToken);
        if (content is null) return null;

        var parsed = GpxParser.TryParseRoute(content);
        if (parsed is not null)
            return (parsed, url);

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
            logger.LogDebug(ex, "ScrapeRaceWorker: could not fetch {Url}", url);
            return null;
        }
    }
}
