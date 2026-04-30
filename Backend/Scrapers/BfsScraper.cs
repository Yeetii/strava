using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;

namespace Backend.Scrapers;

// Performs a breadth-first search from a race website URL to discover GPX route files.
// Depth 3 / bounded by a total crawl budget.
internal sealed class BfsScraper(ILogger logger)
{
    private readonly RaceDayMapScraper _raceDayMap = new(logger);
    private readonly RideWithGpsScraper _rideWithGps = new(logger);
    private const int MaxDepth = 3;
    private static readonly TimeSpan CrawlBudget = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan MaxSingleRequestTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan MinRemainingBudget = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// BFS-scrapes each URL in <paramref name="urls"/> and merges the results.
    /// </summary>
    public async Task<RaceScraperResult?> ScrapeAsync(
        IReadOnlyList<Uri> urls,
        HttpClient httpClient,
        CancellationToken cancellationToken)
    {
        if (urls.Count == 0) return null;

        var filtered = urls.Where(u => !OrganizerUrlRules.IsBareSocialDomain(u) && !OrganizerUrlRules.IsBlockedMediaDomain(u)).ToList();
        if (filtered.Count == 0) return null;

        return await ScrapeSiteAsync(filtered[0], filtered, httpClient, cancellationToken);
    }

    private async Task<RaceScraperResult?> ScrapeSiteAsync(
        Uri startUrl,
        IReadOnlyList<Uri> scrapeUrls,
        HttpClient httpClient,
        CancellationToken cancellationToken)
    {
        var deadlineUtc = DateTimeOffset.UtcNow.Add(CrawlBudget);
        var bfsResult = await FindGpxRoutesAsync(httpClient, startUrl, scrapeUrls, deadlineUtc, cancellationToken);
        var (imageUrl, logoUrl, startPageDate, startPageElevation, startFee, currency, startPageRaceType, _, _) =
            ExtractCoursePageMetadata(bfsResult.StartPageHtml ?? string.Empty, startUrl, bfsResult.SupplementaryContent);
        var extractedName = RaceHtmlScraper.ExtractEventName(startUrl, bfsResult.StartPageHtml,
            [.. bfsResult.CoursePages.Select(cp => (cp.Url, cp.Html))]);

        // If any RaceDayMap embeds were found, scrape them and return the result directly
        // (taking precedence over BFS-discovered routes for embedded map pages).
        if (bfsResult.RaceDayMapSlugs.Count > 0)
        {
            var rdmRoutes = new List<ScrapedRoute>();
            foreach (var slug in bfsResult.RaceDayMapSlugs)
            {
                var slugRoutes = await _raceDayMap.ScrapeSlugAsync(slug, startUrl, httpClient, cancellationToken);
                if (slugRoutes is not null)
                    rdmRoutes.AddRange(slugRoutes);
            }

            if (rdmRoutes.Count > 0)
            {
                var enriched = rdmRoutes.Select(r => r with
                {
                    Date = r.Date ?? startPageDate,
                    ElevationGain = r.ElevationGain ?? startPageElevation,
                    ImageUrl = r.ImageUrl ?? imageUrl,
                    LogoUrl = r.LogoUrl ?? logoUrl,
                    StartFee = r.StartFee ?? startFee,
                    Currency = r.Currency ?? currency,
                    RaceType = r.RaceType ?? startPageRaceType,
                }).ToList();
                var rdmDate = enriched[0].Date;
                return new RaceScraperResult(enriched, ImageUrl: imageUrl, LogoUrl: logoUrl,
                    ExtractedName: extractedName, ExtractedDate: rdmDate,
                    StartFee: startFee, Currency: currency);
            }
        }

        // If any RideWithGPS embeds were found, scrape them and return the result directly.
        if (bfsResult.RideWithGpsRouteIds.Count > 0)
        {
            var rwgpsRoutes = new List<ScrapedRoute>();
            foreach (var routeId in bfsResult.RideWithGpsRouteIds)
            {
                var route = await _rideWithGps.ScrapeRouteAsync(routeId, startUrl, httpClient, cancellationToken);
                if (route is not null)
                    rwgpsRoutes.Add(route);
            }

            if (rwgpsRoutes.Count > 0)
            {
                var enriched = rwgpsRoutes.Select(r => r with
                {
                    Date = r.Date ?? startPageDate,
                    ImageUrl = r.ImageUrl ?? imageUrl,
                    LogoUrl = r.LogoUrl ?? logoUrl,
                    StartFee = r.StartFee ?? startFee,
                    Currency = r.Currency ?? currency,
                    RaceType = r.RaceType ?? startPageRaceType,
                }).ToList();
                return new RaceScraperResult(enriched, ImageUrl: imageUrl, LogoUrl: logoUrl,
                    ExtractedName: extractedName, ExtractedDate: startPageDate,
                    StartFee: startFee, Currency: currency);
            }
        }

        // Build the candidate source list. GPX routes take priority; fall back to coordinate-less
        // course pages when no GPX was found. Both go through the same BuildRoute helper so
        // metadata extraction is identical — GPX routes just supply more accurate distance/name.
        List<ScrapedRoute> routes;

        if (bfsResult.Routes.Count > 0)
        {
            routes = [.. bfsResult.Routes
                .Where(r => IsSameDomain(r.SourcePageUrl, startUrl))
                .Select(r => BuildRoute(r.SourcePageUrl, r.SourceHtml ?? string.Empty,
                    bfsResult.StartPageHtml, r.Route.Coordinates,
                    GpxParser.CalculateDistanceKm(r.Route.Coordinates), r.GpxUrl, startUrl))];

            var preDedupRouteCount = routes.Count;

            // Deduplicate: if two routes share the same name and GPX distances within 10%, keep the first.
            var seen = new List<(string Name, double DistanceKm)>();
            var deduped = new List<ScrapedRoute>(routes.Count);
            foreach (var r in routes)
            {
                var name = (r.Name ?? "").Trim();
                var km = RaceDistanceKm.TryParsePrimarySegmentKilometers(r.Distance) ?? 0.0;
                if (km > 0 && seen.Any(s => s.Name.Equals(name, StringComparison.OrdinalIgnoreCase)
                        && RaceDistanceKm.WithinRelativeOfReference(km, s.DistanceKm, 0.10)))
                {
                    logger.LogDebug("BFS: deduplicating route '{Name}' ({Km:0.#} km) — already seen", name, km);
                    continue;
                }
                seen.Add((name, km));
                deduped.Add(r);
            }
            routes = deduped;

            if (routes.Count != preDedupRouteCount)
            {
                logger.LogInformation(
                    "BFS: {Url} — collapsed {Before} GPX-backed route candidates to {After} final routes after name/distance dedupe",
                    startUrl,
                    preDedupRouteCount,
                    routes.Count);
            }
        }
        else
        {
            // No GPX — fall back to coordinate-less course pages.
            // Deduplicate by rounded distance before building routes.
            var sameDomain = bfsResult.CoursePages
                .Where(cp => IsSameDomain(cp.Url, startUrl))
                .ToList();

            var dedupedPages = sameDomain
                .GroupBy(cp =>
                {
                    var km = RaceDistanceKm.TryParsePrimarySegmentKilometers(cp.Distance);
                    return km.HasValue ? (int)Math.Round(km.Value) : 0;
                })
                .SelectMany(g => g.Key == 0
                    ? g.AsEnumerable()
                    : [g.OrderBy(cp => cp.IsContentOnly ? 1 : 0).First()])
                .OrderBy(cp => cp.IsContentOnly ? 1 : 0)
                .ToList();

            routes = [.. dedupedPages.Select(cp => BuildRoute(cp.Url, cp.Html, bfsResult.StartPageHtml, [], null, null, startUrl))];
        }

        var extractedDate = routes.Count > 0 && bfsResult.Routes.Count > 0
            ? routes[0].Date
            : startPageDate;

        return new RaceScraperResult(routes, ImageUrl: imageUrl, LogoUrl: logoUrl,
            ExtractedName: extractedName, ExtractedDate: extractedDate,
            StartFee: startFee, Currency: currency);
    }

    private static ScrapedRoute BuildRoute(
        Uri sourcePageUrl,
        string sourcePageHtml,
        string? startPageHtml,
        IReadOnlyList<Coordinate> coordinates,
        double? gpxDistanceKm,
        Uri? gpxUrl,
        Uri startUrl)
    {
        var (img, logo, date, elevation, startingFee, curr, raceType, htmlName, htmlDistance) =
            ExtractCoursePageMetadata(sourcePageHtml, sourcePageUrl, null, startPageHtml);
        var name = htmlName ?? "Unnamed";
        // When a GPX distance is available, snap to the HTML label within tolerance; otherwise
        // fall back to the HTML-extracted distance string.
        var resolvedDistance = gpxDistanceKm.HasValue
            ? (RaceScrapeDiscovery.MatchDistanceKmToVerbose(gpxDistanceKm.Value, htmlDistance, 0.10)
               ?? RaceScrapeDiscovery.FormatDistanceKm(gpxDistanceKm.Value))
            : htmlDistance;
        return new ScrapedRoute(
            Coordinates: coordinates,
            SourceUrl: sourcePageUrl,
            Name: name,
            Distance: resolvedDistance,
            ElevationGain: elevation,
            GpxUrl: gpxUrl,
            ImageUrl: img is not null && OrganizerUrlRules.IsBlockedMediaDomain(img) ? null : img,
            LogoUrl: logo is not null && OrganizerUrlRules.IsBlockedMediaDomain(logo) ? null : logo,
            Date: date,
            StartFee: startingFee,
            Currency: curr,
            GpxSource: gpxUrl is not null ? GpxSourceResolver.Resolve(gpxUrl, startUrl) : null,
            RaceType: raceType);
    }

    private record BfsResult(
        IReadOnlyList<(ParsedGpxRoute Route, Uri GpxUrl, Uri SourcePageUrl, string? SourceHtml)> Routes,
        string? StartPageHtml,
        IReadOnlyList<CoursePage> CoursePages,
        string? SupplementaryContent,
        IReadOnlyList<string> RaceDayMapSlugs,
        IReadOnlyList<string> RideWithGpsRouteIds);

    private record CoursePage(Uri Url, string Html, string? Distance, bool IsContentOnly = false);

    private static (Uri? ImageUrl, Uri? LogoUrl, string? Date, double? ElevationGain, string? StartFee, string? Currency, string? RaceType, string? Name, string? Distance) ExtractCoursePageMetadata(
        string html,
        Uri pageUrl,
        string? supplementaryContent,
        string? startPageHtml = null)
    {
        var combined = supplementaryContent is not null ? html + "\n" + supplementaryContent : html;
        var imageUrl = RaceHtmlScraper.ExtractProminentImage(combined, pageUrl);
        var logoUrl = RaceHtmlScraper.ExtractLogo(html, pageUrl);
        var date = RaceHtmlScraper.ExtractDate(html);
        var elevation = RaceHtmlScraper.ExtractElevationGain(html);
        var price = RaceHtmlScraper.ExtractPrice(html, pageUrl);

        // Use start page HTML as fallback source and for name-segment filtering.
        var startPageName = startPageHtml is not null ? RaceHtmlScraper.ExtractNameCandidate(startPageHtml) : null;
        if (startPageHtml is not null)
        {
            imageUrl ??= RaceHtmlScraper.ExtractProminentImage(startPageHtml, pageUrl);
            logoUrl ??= RaceHtmlScraper.ExtractLogo(startPageHtml, pageUrl);
            date ??= RaceHtmlScraper.ExtractDate(startPageHtml);
            elevation ??= RaceHtmlScraper.ExtractElevationGain(startPageHtml);
            price ??= RaceHtmlScraper.ExtractPrice(startPageHtml, pageUrl);
        }

        var name = RaceHtmlScraper.ExtractName(html, pageUrl, startPageName);
        var contentDistances = RaceHtmlScraper.ExtractDistancesFromContent(html);
        var distance = RaceHtmlScraper.ExtractDistanceFromUrl(pageUrl)
                   ?? (contentDistances.Count > 0 ? string.Join(", ", contentDistances) : null);

        var urlPath = pageUrl.AbsolutePath;
        var raceType = ExtractRaceType(urlPath, name);

        return (imageUrl, logoUrl, date, elevation, price?.Amount, price?.Currency, raceType, name, distance);
    }

    private static readonly string[] KnownRaceTypes = ["trail", "road", "vertical", "triathlon", "backyard", "swimrun", "duathlon"];

    private static string? ExtractRaceType(string urlPath, string? name)
    {
        var matched = KnownRaceTypes.Where(t =>
            urlPath.Contains(t, StringComparison.OrdinalIgnoreCase)
            || (name is not null && name.Contains(t, StringComparison.OrdinalIgnoreCase)));
        var result = string.Join(", ", matched);
        return result.Length > 0 ? result : null;
    }

    /// <summary>Strip the fragment (#…) so URLs differing only by anchor are treated as the same page.</summary>
    private static string StripFragment(Uri uri)
    {
        var abs = uri.AbsoluteUri;
        var idx = abs.IndexOf('#');
        return idx >= 0 ? abs[..idx] : abs;
    }

    // BFS traversal (level-parallel): visits pages, collects GPX links, returns parsed routes.
    private async Task<BfsResult> FindGpxRoutesAsync(
        HttpClient httpClient,
        Uri startUrl,
        IReadOnlyList<Uri> scrapeUrls,
        DateTimeOffset deadlineUtc,
        CancellationToken cancellationToken)
    {
        var visitedPages = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        // Maps GPX URL → source page info (first page that referenced the GPX wins).
        var gpxUrlToPage = new Dictionary<string, (string Html, Uri PageUrl)>(StringComparer.OrdinalIgnoreCase);
        // Track HTML content for pages that had GPX links (for image extraction).
        var pagesWithGpx = new List<(Uri PageUri, string Html)>();
        // Track pages reached via course links that have a distance in their URL.
        var coursePages = new List<CoursePage>();
        // Track external "GPX"-labelled links to probe after the BFS loop.
        // Maps external URL → (source page HTML, source page URL) so findings are attributed to the internal page.
        var externalGpxProbes = new Dictionary<string, (string Html, Uri PageUrl)>(StringComparer.OrdinalIgnoreCase);
        // Google Drive folder URLs found during BFS — resolved to download links after the BFS loop.
        var driveFolderProbes = new Dictionary<string, (string Html, Uri PageUrl)>(StringComparer.OrdinalIgnoreCase);
        // Dropbox shared folders — downloaded as zip after BFS; GPX entries stored for inline fetch.
        var dropboxFolderProbes = new Dictionary<string, (string Html, Uri PageUrl)>(StringComparer.OrdinalIgnoreCase);
        // Same-domain JS bundles to probe post-crawl for embedded GPX paths (SPAs like Vite/React).
        var scriptBundleProbes = new Dictionary<string, (string Html, Uri PageUrl)>(StringComparer.OrdinalIgnoreCase);
        var inlineGpxByUrl = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        // RaceDayMap slugs found in iframe embeds on any visited page.
        var raceDayMapSlugs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        // RideWithGPS route IDs found in iframe embeds on any visited page.
        var rwgpsRouteIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        string? startPageHtml = null;

        // Process one depth level at a time, fetching all pages in the level concurrently.
        var currentLevel = scrapeUrls.Select(u => (Url: u, IsCourseLink: false)).ToList();

        for (int depth = 0; depth <= MaxDepth && currentLevel.Count > 0 && HasRemainingBudget(deadlineUtc); depth++)
        {
            var pagesToFetch = new List<(Uri Url, bool IsCourseLink)>();
            foreach (var entry in currentLevel)
            {
            if (!HasRemainingBudget(deadlineUtc)) break;
                if (visitedPages.Add(StripFragment(entry.Url)))
                    pagesToFetch.Add(entry);
            }

            if (pagesToFetch.Count == 0) break;

            logger.LogDebug("BFS depth {Depth}: fetching {Count} pages", depth, pagesToFetch.Count);
            foreach (var p in pagesToFetch)
                logger.LogDebug("BFS depth {Depth}: {Url}", depth, p.Url);

            var fetchTasks = pagesToFetch.Select(e => TryFetchStringAsync(httpClient, e.Url, deadlineUtc, cancellationToken));
            var htmlResults = await Task.WhenAll(fetchTasks);
            logger.LogDebug("BFS depth {Depth}: fetched {Ok}/{Total} pages", depth, htmlResults.Count(r => r is not null), htmlResults.Length);

            var nextLevel = new List<(Uri Url, bool IsCourseLink)>();
            for (int i = 0; i < pagesToFetch.Count; i++)
            {
                var html = htmlResults[i];
                if (html is null) continue;

                var pageUrl = pagesToFetch[i].Url;
                var isCourseLink = pagesToFetch[i].IsCourseLink;

                if (pageUrl.AbsoluteUri.Equals(startUrl.AbsoluteUri, StringComparison.OrdinalIgnoreCase))
                    startPageHtml = html;

                // Visiting a cloud folder page directly (e.g. scrape URL is only the share link).
                if (IsGoogleDriveFolder(pageUrl))
                    driveFolderProbes.TryAdd(pageUrl.AbsoluteUri, (html, pageUrl));
                if (DropboxShareParser.IsDropboxSharedFolder(pageUrl))
                    dropboxFolderProbes.TryAdd(pageUrl.AbsoluteUri, (html, pageUrl));

                // Collect any RaceDayMap embed slugs found on this page.
                foreach (var slug in RaceDayMapScraper.ExtractSlugs(html))
                    raceDayMapSlugs.Add(slug);

                // Garmin Connect activity/share pages do not expose a direct .gpx link in HTML,
                // but their activity ID maps to a stable GPX export endpoint.
                if (TryGetGarminConnectGpxDownloadUrl(pageUrl, out var garminGpxDownloadUrl))
                    gpxUrlToPage.TryAdd(garminGpxDownloadUrl.AbsoluteUri, (html, pageUrl));

                // Collect any RideWithGPS route IDs found in iframes on this page.
                foreach (var routeId in RideWithGpsScraper.ExtractRouteIds(html))
                    rwgpsRouteIds.Add(routeId);

                // Collect external iframe src URLs — probe them for GPX the same way as other external links.
                foreach (var iframeSrc in RaceHtmlScraper.ExtractIframeSrcLinksFromHtml(html, pageUrl))
                {
                    if (!IsSameDomain(iframeSrc, startUrl)
                        && !iframeSrc.Host.Equals("ridewithgps.com", StringComparison.OrdinalIgnoreCase)
                        && !iframeSrc.Host.Equals("app.racedaymap.com", StringComparison.OrdinalIgnoreCase)
                        && !OrganizerUrlRules.IsBareSocialDomain(iframeSrc)
                        && !OrganizerUrlRules.IsBlockedMediaDomain(iframeSrc))
                        externalGpxProbes.TryAdd(iframeSrc.AbsoluteUri, (html, pageUrl));
                }

                var pageGpxLinks = RaceHtmlScraper.ExtractGpxLinksFromHtml(html, pageUrl);
                foreach (var gpxLink in pageGpxLinks)
                {
                    if (IsGoogleDriveFolder(gpxLink))
                    {
                        driveFolderProbes.TryAdd(gpxLink.AbsoluteUri, (html, pageUrl));
                    }
                    else if (TryGetGoogleDriveFileDownloadUrl(gpxLink, out var driveFileDownloadUrl))
                    {
                        // Direct Drive file link (e.g. /file/d/{ID}/view) — convert to download URL.
                        gpxUrlToPage.TryAdd(driveFileDownloadUrl.AbsoluteUri, (html, pageUrl));
                    }
                    else if (DropboxShareParser.IsDropboxSharedFolder(gpxLink))
                    {
                        dropboxFolderProbes.TryAdd(gpxLink.AbsoluteUri, (html, pageUrl));
                    }
                    else if (IsSameDomain(gpxLink, startUrl) || gpxLink.AbsolutePath.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase))
                    {
                        // Same-domain GPX links or direct .gpx files — handle normally.
                        gpxUrlToPage.TryAdd(gpxLink.AbsoluteUri, (html, pageUrl));
                    }
                    else
                    {
                        // External non-.gpx link with "GPX" in its text — probe later, attribute to this page.
                        externalGpxProbes.TryAdd(gpxLink.AbsoluteUri, (html, pageUrl));
                    }
                }

                // Use helper to detect and add course pages
                var detectedPage = DetectCoursePage(pageUrl, html, depth);
                if (detectedPage is not null)
                    coursePages.Add(detectedPage);

                // Pick up Google Drive folder/file links from download-text anchors on the page.
                foreach (var dlLink in RaceHtmlScraper.ExtractDownloadLinksFromHtml(html, pageUrl))
                {
                    if (IsGoogleDriveFolder(dlLink))
                        driveFolderProbes.TryAdd(dlLink.AbsoluteUri, (html, pageUrl));
                    else if (TryGetGoogleDriveFileDownloadUrl(dlLink, out var driveFileDlUrl))
                        gpxUrlToPage.TryAdd(driveFileDlUrl.AbsoluteUri, (html, pageUrl));
                    else if (DropboxShareParser.IsDropboxSharedFolder(dlLink))
                        dropboxFolderProbes.TryAdd(dlLink.AbsoluteUri, (html, pageUrl));
                }

                // Links whose text looks like a distance ("25 km") may point to GPX files
                // with no .gpx extension. Probe same-domain ones directly; treat cross-domain
                // ones as external probes (capped by Take(5) later).
                foreach (var kmLink in RaceHtmlScraper.ExtractKmDistanceCandidateLinksFromHtml(html, pageUrl))
                {
                    if (IsSameDomain(kmLink, startUrl))
                        gpxUrlToPage.TryAdd(kmLink.AbsoluteUri, (html, pageUrl));
                    else
                        externalGpxProbes.TryAdd(kmLink.AbsoluteUri, (html, pageUrl));
                }

                // Collect same-domain JS bundles to probe for embedded GPX paths (SPAs).
                foreach (var scriptUrl in RaceHtmlScraper.ExtractScriptSrcLinksFromHtml(html, pageUrl))
                {
                    if (IsSameDomain(scriptUrl, startUrl))
                        scriptBundleProbes.TryAdd(scriptUrl.AbsoluteUri, (html, pageUrl));
                }

                if (depth < MaxDepth)
                {
                    foreach (var courseLink in RaceHtmlScraper.ExtractCourseLinksFromHtml(html, pageUrl))
                    {
                        if (!visitedPages.Contains(StripFragment(courseLink))
                            && IsSameDomain(courseLink, startUrl)
                            && OrganizerUrlRules.CanBfsCrawlUri(courseLink, OrganizerUrlRules.DeriveOrganizerKey(startUrl)))
                            nextLevel.Add((courseLink, true));
                    }
                }
            }

            currentLevel = nextLevel;
        }

        logger.LogDebug("BFS crawl done. Visited {Pages} pages, found {Gpx} GPX URLs, {ExtProbes} external probes, {Scripts} JS bundles",
            visitedPages.Count, gpxUrlToPage.Count, externalGpxProbes.Count, scriptBundleProbes.Count);

        // Probe external links: fetch the page, then follow one level of same-domain course links.
        // This handles embedded event platforms where the iframe root links onward to the actual
        // route page (for example `/app` -> `/page/bana`). Any findings are attributed back to
        // the internal page that linked to the external site.
        foreach (var (externalUrl, (sourceHtml, sourcePageUrl)) in externalGpxProbes.Take(5))
        {
            if (!HasRemainingBudget(deadlineUtc)) break;
            logger.LogDebug("BFS: probing external link {Url}", externalUrl);
            if (gpxUrlToPage.ContainsKey(externalUrl)) continue;

            var probeUri = new Uri(externalUrl);
            var externalContent = await TryFetchStringAsync(httpClient, probeUri, deadlineUtc, cancellationToken);
            if (externalContent is null) continue;

            // If the external URL returned GPX directly, register it with the internal source page.
            if (GpxParser.TryParseRoute(externalContent) is not null)
            {
                gpxUrlToPage.TryAdd(externalUrl, (sourceHtml, sourcePageUrl));
                continue;
            }

            var pagesToProcess = new List<(Uri Url, string Html)> { (probeUri, externalContent) };
            var subLinks = RaceHtmlScraper.ExtractCourseLinksFromHtml(externalContent, probeUri)
                .Where(u => IsSameDomain(u, probeUri))
                .Take(5)
                .ToList();

            if (subLinks.Count > 0)
            {
                logger.LogDebug("BFS: external link {Url} — fetching {Count} same-domain sub-links", externalUrl, subLinks.Count);
                var subTasks = subLinks.Select(u => TryFetchStringAsync(httpClient, u, deadlineUtc, cancellationToken));
                var subResults = await Task.WhenAll(subTasks);
                for (int i = 0; i < subLinks.Count; i++)
                {
                    if (subResults[i] is not null)
                        pagesToProcess.Add((subLinks[i], subResults[i]!));
                }
            }

            foreach (var (pageUri, pageHtml) in pagesToProcess)
            {
                if (TryGetGarminConnectGpxDownloadUrl(pageUri, out var garminGpxDownloadUrl))
                {
                    gpxUrlToPage.TryAdd(garminGpxDownloadUrl.AbsoluteUri, (sourceHtml, sourcePageUrl));

                    // Public Garmin activity pages expose enough metadata to build a fallback
                    // route even when the export endpoint redirects to sign-in HTML.
                    var garminCoursePage = DetectCoursePage(pageUri, pageHtml, 1);
                    if (garminCoursePage is not null)
                        coursePages.Add(garminCoursePage with { Url = sourcePageUrl, Html = pageHtml });
                }

                foreach (var slug in RaceDayMapScraper.ExtractSlugs(pageHtml))
                    raceDayMapSlugs.Add(slug);
                foreach (var routeId in RideWithGpsScraper.ExtractRouteIds(pageHtml))
                    rwgpsRouteIds.Add(routeId);

                var innerGpxLinks = RaceHtmlScraper.ExtractGpxLinksFromHtml(pageHtml, pageUri)
                    .Concat(RaceHtmlScraper.ExtractDownloadLinksFromHtml(pageHtml, pageUri))
                    .Take(10);

                foreach (var innerLink in innerGpxLinks)
                    gpxUrlToPage.TryAdd(innerLink.AbsoluteUri, (sourceHtml, sourcePageUrl));
            }
        }

        logger.LogDebug("BFS: external probes done. Total GPX URLs: {Count}", gpxUrlToPage.Count);

        // Probe same-domain JS bundles for embedded GPX paths (e.g. Vite/React SPAs that render links via JS).
        // Bundle content is also accumulated for image extraction (background images etc. in JS-rendered sites).
        var scriptBundleContent = new List<string>();
        foreach (var (scriptUrl, (sourceHtml, sourcePageUrl)) in scriptBundleProbes.Take(5))
        {
            if (!HasRemainingBudget(deadlineUtc)) break;
            logger.LogDebug("BFS: probing JS bundle for GPX links: {Url}", scriptUrl);
            var scriptContent = await TryFetchStringAsync(httpClient, new Uri(scriptUrl), deadlineUtc, cancellationToken);
            if (scriptContent is null) continue;
            foreach (var gpxUri in RaceHtmlScraper.ExtractGpxUrlsFromHtml(scriptContent, sourcePageUrl))
                gpxUrlToPage.TryAdd(gpxUri.AbsoluteUri, (sourceHtml, sourcePageUrl));
            scriptBundleContent.Add(scriptContent);
        }

        if (scriptBundleProbes.Count > 0)
            logger.LogDebug("BFS: JS bundle probes done. Total GPX URLs: {Count}", gpxUrlToPage.Count);

        // Probe Google Drive folder links: recursively crawl subfolders (skipping ignored names)
        // and collect download URLs. Attributed to the internal page that linked to the folder.
        foreach (var (folderUrl, (sourceHtml, sourcePageUrl)) in driveFolderProbes.Take(5))
        {
            if (!HasRemainingBudget(deadlineUtc)) break;
            logger.LogDebug("BFS: probing Google Drive folder {Url}", folderUrl);
            var downloadUrls = await CrawlGoogleDriveFolderAsync(httpClient, new Uri(folderUrl), deadlineUtc, cancellationToken);
            logger.LogDebug("BFS: Drive folder yielded {Count} download URLs", downloadUrls.Count);
            foreach (var dlUrl in downloadUrls)
                gpxUrlToPage.TryAdd(dlUrl.AbsoluteUri, (sourceHtml, sourcePageUrl));
        }

        if (driveFolderProbes.Count > 0)
            logger.LogDebug("BFS: Drive folder probes done. Total GPX URLs: {Count}", gpxUrlToPage.Count);

        // Dropbox shared folders: dl=1 returns a zip; extract .gpx files and register clickable folder URLs + fragment per entry.
        foreach (var (folderUrl, (sourceHtml, sourcePageUrl)) in dropboxFolderProbes.Take(5))
        {
            if (!HasRemainingBudget(deadlineUtc)) break;
            logger.LogDebug("BFS: probing Dropbox shared folder {Url}", folderUrl);
            var folderUri = new Uri(folderUrl);
            var zipBytes = await TryFetchDropboxFolderZipAsync(httpClient, folderUri, cancellationToken);
            if (zipBytes is null) continue;

            var extracted = DropboxShareParser.ExtractGpxFromZip(zipBytes);
            logger.LogDebug("BFS: Dropbox folder yielded {Count} GPX entries from zip", extracted.Count);
            foreach (var (entryPath, gpxXml) in extracted)
            {
                var entryUri = DropboxShareParser.ToSharedFolderEntryUri(folderUri, entryPath);
                inlineGpxByUrl[entryUri.AbsoluteUri] = gpxXml;
                gpxUrlToPage.TryAdd(entryUri.AbsoluteUri, (sourceHtml, sourcePageUrl));
            }
        }

        if (dropboxFolderProbes.Count > 0)
            logger.LogDebug("BFS: Dropbox folder probes done. Total GPX URLs: {Count}", gpxUrlToPage.Count);

        // Fetch external CSS stylesheets and same-domain JS bundles from the start page so background-image
        // URLs are found by image extraction (CSS) and SPA-embedded asset URLs are also scanned (JS bundles).
        string? supplementaryContent = null;
        if (startPageHtml is not null)
        {
            var cssUrls = RaceHtmlScraper.ExtractStylesheetUrls(startPageHtml, startUrl);
            logger.LogDebug("BFS: found {Count} CSS stylesheets to fetch", cssUrls.Count);
            if (cssUrls.Count > 0)
            {
                var cssTasks = cssUrls.Take(5).Select(u => TryFetchStringAsync(httpClient, u, deadlineUtc, cancellationToken));
                var cssResults = await Task.WhenAll(cssTasks);
                supplementaryContent = string.Join("\n", cssResults.Where(c => c is not null));
                logger.LogDebug("BFS: CSS fetched, total {Len} chars", supplementaryContent.Length);;
            }
        }

        // Append JS bundle content so image extraction can find background images in SPA bundles.
        if (scriptBundleContent.Count > 0)
            supplementaryContent = (supplementaryContent is not null ? supplementaryContent + "\n" : "") + string.Join("\n", scriptBundleContent);

        if (gpxUrlToPage.Count == 0)
            return new BfsResult([], startPageHtml, coursePages, supplementaryContent, [.. raceDayMapSlugs], [.. rwgpsRouteIds]);

        // Fetch all GPX URLs concurrently.
        var gpxTasks = gpxUrlToPage.Keys.Select(url =>
            TryFetchGpxFromUrlAsync(httpClient, new Uri(url), deadlineUtc, cancellationToken, inlineGpxByUrl));
        var gpxResults = await Task.WhenAll(gpxTasks);
        var parsedCount = gpxResults.Count(r => r.HasValue);
        var failedCount = gpxUrlToPage.Count - parsedCount;
        if (failedCount > 0)
        {
            // Only count URLs that looked explicitly like GPX (path ends in .gpx) as genuine failures.
            // Other URLs (e.g. Drive probe downloads that turned out to be non-GPX files) are
            // expected misses and should not produce error-level noise.
            var gpxKeys = gpxUrlToPage.Keys.ToList();
            var genuineFailureCount = gpxKeys
                .Where((url, i) => !gpxResults[i].HasValue
                    && new Uri(url).AbsolutePath.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase))
                .Count();
            if (genuineFailureCount > 0)
                logger.LogError("BFS: {Failed}/{Total} .gpx URLs were fetched but could not be parsed", genuineFailureCount, gpxUrlToPage.Count);
            else
                logger.LogDebug("BFS: {Failed}/{Total} fetched URLs yielded no GPX (non-GPX files skipped)", failedCount, gpxUrlToPage.Count);
        }

        var routes = gpxResults
            .Where(r => r.HasValue)
            .Select(r =>
            {
                var (route, gpxUrl) = r!.Value;
                var (html, sourcePageUrl) = gpxUrlToPage[gpxUrl.AbsoluteUri];
                return (route, gpxUrl, SourcePageUrl: sourcePageUrl, (string?)html);
            })
            // Deduplicate by filename: if two URLs resolve to the same file name (case-insensitive),
            // keep only the first. Uses the last non-empty path segment, or the Drive file ID for uc URLs.
            // For Dropbox folder entry URIs the distinguishing identifier is the fragment (e.g. #100M.gpx),
            // not the path (which is the shared folder ID and identical for all entries).
            .GroupBy(r =>
            {
                var uri = r.gpxUrl;
                if (uri.Host.Equals("drive.google.com", StringComparison.OrdinalIgnoreCase)
                    && uri.AbsolutePath == "/uc")
                {
                    var id = System.Web.HttpUtility.ParseQueryString(uri.Query)["id"];
                    return (id ?? uri.AbsoluteUri).ToLowerInvariant();
                }
                if (DropboxShareParser.IsDropboxSharedFolder(uri) && !string.IsNullOrEmpty(uri.Fragment))
                    return uri.Fragment.ToLowerInvariant();
                var segments = uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
                return (segments.LastOrDefault() ?? uri.AbsoluteUri).ToLowerInvariant();
            })
            .Select(g => g.First())
            .ToList();

        logger.LogInformation(
            "BFS: {Url} — visited {Pages} pages, {GpxFound} GPX parsed, {Routes} unique GPX-backed route candidates",
            startUrl,
            visitedPages.Count,
            parsedCount,
            routes.Count);
        return new BfsResult(routes, startPageHtml, coursePages, supplementaryContent, [.. raceDayMapSlugs], [.. rwgpsRouteIds]);
    }

    private static CoursePage? DetectCoursePage(Uri pageUrl, string html, int depth)
    {
        var urlDistance = RaceHtmlScraper.ExtractDistanceFromUrl(pageUrl);
        if (urlDistance is not null)
            return new CoursePage(pageUrl, html, urlDistance);

        if (depth > 0)
        {
            var contentDistances = RaceHtmlScraper.ExtractDistancesFromContent(html);
            if (contentDistances.Count > 0 && RaceHtmlScraper.ExtractDate(html) is not null)
            {
                var maxDist = contentDistances
                    .OrderByDescending(d =>
                    {
                        var m = System.Text.RegularExpressions.Regex.Match(d, "[\\d.]+");
                        return m.Success && double.TryParse(m.Value,
                            System.Globalization.NumberStyles.Float,
                            System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : 0;
                    })
                    .First();
                return new CoursePage(pageUrl, html, maxDist, IsContentOnly: true);
            }
        }

        return null;
    }

    private async Task<(ParsedGpxRoute Route, Uri GpxUrl)?> TryFetchGpxFromUrlAsync(
        HttpClient httpClient,
        Uri url,
        DateTimeOffset deadlineUtc,
        CancellationToken cancellationToken,
        IReadOnlyDictionary<string, string>? inlineGpxByUrl = null)
    {
        if (inlineGpxByUrl?.TryGetValue(url.AbsoluteUri, out var inlineXml) == true)
        {
            var inlineParsed = GpxParser.TryParseRoute(inlineXml);
            if (inlineParsed is not null)
                return (inlineParsed, url);
        }

        var fetchUrl = DropboxShareParser.IsDropboxHost(url) && DropboxShareParser.IsDropboxSharedFile(url)
            ? DropboxShareParser.WithDl1(url)
            : url;

        var content = await TryFetchStringAsync(httpClient, fetchUrl, deadlineUtc, cancellationToken, inlineGpxByUrl);
        if (content is null) return null;

        var parsed = GpxParser.TryParseRoute(content);
        if (parsed is not null)
            return (parsed, url);

        // The URL returned HTML instead of GPX — try any GPX/download links found within it.
        logger.LogDebug("BFS: {Url} returned HTML, searching for secondary GPX links", url);
        var secondaryUrls = RaceHtmlScraper.ExtractGpxLinksFromHtml(content, url)
            .Concat(RaceHtmlScraper.ExtractDownloadLinksFromHtml(content, url))
            .Select(u => u.AbsoluteUri)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(u => new Uri(u))
            .ToList();

        logger.LogDebug("BFS: {Url} has {Count} secondary URLs", url, secondaryUrls.Count);
        var secondaryTasks = secondaryUrls.Take(10).Select(async link =>
        {
            var linkContent = await TryFetchStringAsync(httpClient, link, deadlineUtc, cancellationToken, inlineGpxByUrl);
            if (linkContent is null) return ((ParsedGpxRoute, Uri)?)null;
            var linkParsed = GpxParser.TryParseRoute(linkContent);
            return linkParsed is not null ? (linkParsed, link) : null;
        });

        var secondaryResults = await Task.WhenAll(secondaryTasks);
        return secondaryResults.FirstOrDefault(r => r.HasValue);
    }

    private const int MaxResponseBytes = 2 * 1024 * 1024; // 2 MB
    private const int MaxGpxResponseBytes = 10 * 1024 * 1024; // 10 MB — GPX files can be large

    private async Task<string?> TryFetchStringAsync(
        HttpClient httpClient,
        Uri url,
        DateTimeOffset deadlineUtc,
        CancellationToken cancellationToken,
        IReadOnlyDictionary<string, string>? inlineGpxByUrl = null)
    {
        if (inlineGpxByUrl?.TryGetValue(url.AbsoluteUri, out var inline) == true)
            return inline;

        var remainingBudget = GetRemainingBudget(deadlineUtc);
        if (remainingBudget is null)
            return null;

        var fetchUri = DropboxShareParser.IsDropboxHost(url) && DropboxShareParser.IsDropboxSharedFile(url)
            ? DropboxShareParser.WithDl1(url)
            : url;

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(remainingBudget.Value < MaxSingleRequestTimeout ? remainingBudget.Value : MaxSingleRequestTimeout);
            using var request = new HttpRequestMessage(HttpMethod.Get, fetchUri);
            if (httpClient.DefaultRequestHeaders.UserAgent.Count == 0)
                request.Headers.UserAgent.ParseAdd("Mozilla/5.0 (compatible; Peakshunters/1.0)");
            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
            response.EnsureSuccessStatusCode();

            // Skip binary content that can't be HTML/GPX/XML.
            var contentType = response.Content.Headers.ContentType?.MediaType ?? "";
            if (contentType.StartsWith("image/", StringComparison.OrdinalIgnoreCase)
                || contentType.StartsWith("video/", StringComparison.OrdinalIgnoreCase)
                || contentType.StartsWith("audio/", StringComparison.OrdinalIgnoreCase)
                || contentType.Equals("application/pdf", StringComparison.OrdinalIgnoreCase)
                || contentType.Equals("application/zip", StringComparison.OrdinalIgnoreCase))
                return null;

            // GPX files (by content-type or .gpx URL path) can be larger than the general HTML limit.
            var isGpx = contentType.Contains("gpx", StringComparison.OrdinalIgnoreCase)
                || url.AbsolutePath.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase);
            var sizeLimit = isGpx ? MaxGpxResponseBytes : MaxResponseBytes;

            // Reject if Content-Length exceeds limit.
            if (response.Content.Headers.ContentLength > sizeLimit)
                return null;

            var bytes = await response.Content.ReadAsByteArrayAsync(cts.Token);
            if (bytes.Length > sizeLimit) return null;
            var text = System.Text.Encoding.UTF8.GetString(bytes);
            // Strip UTF-8 BOM — XmlReader chokes on it when reading from a StringReader.
            if (text.Length > 0 && text[0] == '\uFEFF')
                text = text[1..];
            return text;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw; // Genuine cancellation — propagate.
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "BfsScraper: could not fetch {Url}", fetchUri);
            return null;
        }
    }

    private async Task<byte[]?> TryFetchDropboxFolderZipAsync(
        HttpClient httpClient,
        Uri folderShareUrl,
        CancellationToken cancellationToken)
    {
        var bytes = await DropboxShareParser.TryDownloadSharedFolderZipAsync(httpClient, folderShareUrl, cancellationToken);
        if (bytes is null)
            logger.LogDebug("BfsScraper: could not download Dropbox folder zip {Url}", folderShareUrl);
        return bytes;
    }

    private static readonly string[] IgnoredDriveItemNames = ["stage", "stages", "segment"];

    private static bool IsDriveItemIgnored(string name) =>
        IgnoredDriveItemNames.Any(ignored => name.Contains(ignored, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Recursively crawls a Google Drive folder and its subfolders (up to depth 3),
    /// skipping subfolders whose names match <see cref="IgnoredDriveItemNames"/>.
    /// Returns direct-download URLs for all files found.
    /// <para>
    /// Each <c>data-id</c> found in the folder HTML is probed by fetching it as a
    /// <c>/drive/folders/ID</c> URL. If the response is a Drive folder listing page
    /// (contains further <c>data-id</c> attributes), the item is treated as a subfolder
    /// and recursed into. Otherwise it is treated as a file and a download URL is emitted.
    /// This avoids the need to parse Drive's embedded JSON to distinguish files from folders.
    /// </para>
    /// </summary>
    private async Task<List<Uri>> CrawlGoogleDriveFolderAsync(
        HttpClient httpClient,
        Uri folderUrl,
        DateTimeOffset deadlineUtc,
        CancellationToken cancellationToken,
        HashSet<string>? visited = null,
        int depth = 0,
        string? preloadedHtml = null)
    {
        const int MaxDriveDepth = 3;
        visited ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (depth > MaxDriveDepth || !visited.Add(folderUrl.AbsoluteUri) || !HasRemainingBudget(deadlineUtc))
            return [];

        logger.LogDebug("BFS: crawling Drive folder depth={Depth} {Url}", depth, folderUrl);
        var folderHtml = preloadedHtml ?? await TryFetchStringAsync(httpClient, folderUrl, deadlineUtc, cancellationToken);
        if (folderHtml is null) return [];

        // Collect unique item IDs from this folder page, skipping ones already visited as folders.
        var dataIds = System.Text.RegularExpressions.Regex.Matches(
                folderHtml, @"data-id=""(?<id>[0-9A-Za-z_-]{20,})""")
            .Select(m => m.Groups["id"].Value)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Where(id => !visited.Contains($"https://drive.google.com/drive/folders/{id}"))
            .ToList();

        logger.LogDebug("BFS: Drive folder has {Count} unique item IDs to probe", dataIds.Count);

        // Probe each item concurrently: try fetching as a folder page to detect subfolders.
        var probeTasks = dataIds.Select(async itemId =>
        {
            var candidateUri = new Uri($"https://drive.google.com/drive/folders/{itemId}");
            var html = await TryFetchStringAsync(httpClient, candidateUri, deadlineUtc, cancellationToken);
            return (ItemId: itemId, Html: html);
        });
        var probed = await Task.WhenAll(probeTasks);

        var results = new List<Uri>();
        foreach (var (itemId, itemHtml) in probed)
        {
            if (itemHtml is not null && IsDriveFolderHtml(itemHtml))
            {
                // It's a subfolder — get its name from the page title and maybe recurse.
                var subfolderUri = new Uri($"https://drive.google.com/drive/folders/{itemId}");
                var name = ExtractDriveFolderName(itemHtml);
                if (IsDriveItemIgnored(name))
                {
                    logger.LogDebug("BFS: skipping Drive subfolder '{Name}'", name);
                    continue;
                }
                logger.LogDebug("BFS: recursing into Drive subfolder '{Name}'", name);
                var sub = await CrawlGoogleDriveFolderAsync(
                    httpClient, subfolderUri, deadlineUtc, cancellationToken, visited, depth + 1, itemHtml);
                results.AddRange(sub);
            }
            else
            {
                // It's a file — emit a download URL using the classic uc endpoint (usercontent returns 500).
                results.Add(new Uri($"https://drive.google.com/uc?export=download&confirm=t&id={itemId}"));
            }
        }

        return results;
    }

    /// <summary>
    /// Returns true when the HTML looks like a Google Drive folder listing page
    /// (i.e. contains item <c>data-id</c> attributes).
    /// </summary>
    private static bool IsDriveFolderHtml(string html) =>
        System.Text.RegularExpressions.Regex.IsMatch(html, @"data-id=""[0-9A-Za-z_-]{20,}""");

    /// <summary>
    /// Extracts the folder name from a Drive folder page title
    /// ("FolderName – Google Drive" → "FolderName").
    /// </summary>
    private static string ExtractDriveFolderName(string html)
    {
        var m = System.Text.RegularExpressions.Regex.Match(
            html,
            @"<title[^>]*>([^<]+?)\s*[–\-—]\s*Google\s+Drive\s*</title>",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        return m.Success ? System.Net.WebUtility.HtmlDecode(m.Groups[1].Value.Trim()) : string.Empty;
    }

    /// <summary>
    /// Checks whether <paramref name="uri"/> is a Google Drive file view link (e.g. /file/d/{ID}/view)
    /// and, if so, outputs the corresponding direct download URL.
    /// </summary>
    private static bool TryGetGoogleDriveFileDownloadUrl(Uri uri, out Uri downloadUrl)
    {
        downloadUrl = null!;
        if (!uri.Host.Equals("drive.google.com", StringComparison.OrdinalIgnoreCase))
            return false;
        var m = System.Text.RegularExpressions.Regex.Match(
            uri.AbsolutePath,
            @"^/file/d/([0-9A-Za-z_-]+)",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (!m.Success) return false;
        downloadUrl = new Uri($"https://drive.google.com/uc?export=download&confirm=t&id={m.Groups[1].Value}");
        return true;
    }

    /// <summary>
    /// Checks whether a URL points to a Garmin Connect activity/course page and, if so,
    /// returns the corresponding GPX export URL.
    /// </summary>
    private static bool TryGetGarminConnectGpxDownloadUrl(Uri uri, out Uri downloadUrl)
    {
        downloadUrl = null!;

        var host = uri.Host;
        var isGarminHost = host.Equals("connect.garmin.com", StringComparison.OrdinalIgnoreCase)
            || host.EndsWith(".garmin.com", StringComparison.OrdinalIgnoreCase);
        if (!isGarminHost)
            return false;

        var activityMatch = System.Text.RegularExpressions.Regex.Match(
            uri.AbsolutePath,
            @"^/(?:app|modern)/activity/(?<id>\d+)(?:/share/\d+)?/?$",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        if (activityMatch.Success)
        {
            downloadUrl = new Uri($"https://connect.garmin.com/app/proxy/download-service/export/gpx/activity/{activityMatch.Groups["id"].Value}");
            return true;
        }

        var courseMatch = System.Text.RegularExpressions.Regex.Match(
            uri.AbsolutePath,
            @"^/(?:app|modern)/course/(?<id>\d+)/?$",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (!courseMatch.Success)
            return false;

        downloadUrl = new Uri($"https://connect.garmin.com/app/proxy/course-service/course/{courseMatch.Groups["id"].Value}/download");
        return true;
    }

    /// <summary>Checks whether a URL points to a Google Drive folder.</summary>
    private static bool IsGoogleDriveFolder(Uri uri) =>
        uri.Host.Equals("drive.google.com", StringComparison.OrdinalIgnoreCase)
        && uri.AbsolutePath.StartsWith("/drive/folders/", StringComparison.OrdinalIgnoreCase);

    /// <summary>Checks that both URIs share the same registrable domain (ignoring www prefix).</summary>
    private static bool IsSameDomain(Uri candidate, Uri origin)
    {
        return OrganizerUrlRules.NormalizeHost(candidate.Host).Equals(OrganizerUrlRules.NormalizeHost(origin.Host), StringComparison.OrdinalIgnoreCase);
    }

    private static bool HasRemainingBudget(DateTimeOffset deadlineUtc)
        => GetRemainingBudget(deadlineUtc) is { } remaining && remaining > TimeSpan.Zero;

    private static TimeSpan? GetRemainingBudget(DateTimeOffset deadlineUtc)
    {
        var remaining = deadlineUtc - DateTimeOffset.UtcNow;
        return remaining > MinRemainingBudget ? remaining : null;
    }
}
