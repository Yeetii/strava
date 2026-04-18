using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend.Scrapers;

// Performs a breadth-first search from a race website URL to discover GPX route files.
// Depth 3 / max 30 pages.
internal sealed class BfsScraper(ILogger logger) : IRaceScraper
{
    private const int MaxDepth = 3;
    private const int MaxPages = 30;

    /// <summary>Checks that both URIs share the same registrable domain (ignoring www prefix).</summary>
    private static bool IsSameDomain(Uri candidate, Uri origin)
    {
        static string NormalizeHost(string host) =>
            host.StartsWith("www.", StringComparison.OrdinalIgnoreCase) ? host[4..] : host;

        return NormalizeHost(candidate.Host).Equals(NormalizeHost(origin.Host), StringComparison.OrdinalIgnoreCase);
    }

    public bool CanHandle(ScrapeJob job) => job.WebsiteUrl is not null;

    public async Task<RaceScraperResult?> ScrapeAsync(ScrapeJob job, HttpClient httpClient, CancellationToken cancellationToken)
    {
        if (job.WebsiteUrl is null) return null;
        return await ScrapeFromUrlAsync(job, job.WebsiteUrl, httpClient, cancellationToken);
    }

    // Runs BFS from an explicit URL.  Called by TraceDeTrailEventScraper as well.
    public async Task<RaceScraperResult?> ScrapeFromUrlAsync(ScrapeJob job, Uri startUrl, HttpClient httpClient, CancellationToken cancellationToken)
    {
        var bfsResult = await FindGpxRoutesAsync(httpClient, startUrl, cancellationToken);
        var eventName = bfsResult.ExtractedName ?? job.Name ?? job.Location ?? "Unnamed";

        if (bfsResult.Routes.Count == 0)
        {
            // No GPX found — return course pages as coordinate-less routes if available.
            if (bfsResult.CoursePages.Count > 0)
            {
                var fallbackDate = bfsResult.StartPageDate;

                // Deduplicate by rounded distance: prefer URL-distance pages over content-only ones,
                // then prefer entries with more metadata.
                var dedupedCoursePages = bfsResult.CoursePages
                    .GroupBy(cp =>
                    {
                        if (cp.Distance is null) return 0;
                        var numMatch = System.Text.RegularExpressions.Regex.Match(cp.Distance, @"[\d.]+");
                        return numMatch.Success && double.TryParse(numMatch.Value,
                            System.Globalization.NumberStyles.Float,
                            System.Globalization.CultureInfo.InvariantCulture, out var km)
                            ? (int)Math.Round(km) : 0;
                    })
                    .SelectMany(g => g.Key == 0
                        ? g.AsEnumerable() // can't dedup without a distance
                        : new[] { g.OrderBy(cp => cp.IsContentOnly ? 1 : 0).First() })
                    .OrderBy(cp => cp.IsContentOnly ? 1 : 0)
                    .ToList();

                var courses = dedupedCoursePages.Select(cp =>
                {
                    var pageImage = RaceHtmlScraper.ExtractProminentImage(cp.Html, cp.Url);
                    var pageDate = RaceHtmlScraper.ExtractDate(cp.Html) ?? fallbackDate;
                    var pageElevation = RaceHtmlScraper.ExtractElevationGain(cp.Html) ?? bfsResult.StartPageElevation;
                    var pagePrice = RaceHtmlScraper.ExtractPrice(cp.Html, cp.Url);
                    var name = cp.Distance is not null
                        ? $"{eventName} {cp.Distance}"
                        : eventName;
                    return new ScrapedRoute(
                        Coordinates: [],
                        SourceUrl: cp.Url,
                        Name: name,
                        Distance: cp.Distance,
                        ElevationGain: pageElevation,
                        ImageUrl: pageImage ?? bfsResult.ImageUrl,
                        LogoUrl: bfsResult.LogoUrl,
                        Date: pageDate,
                        StartFee: pagePrice?.Amount.ToString() ?? bfsResult.StartFee,
                        Currency: pagePrice?.Currency ?? bfsResult.Currency);
                }).ToList();

                return new RaceScraperResult(courses, ImageUrl: bfsResult.ImageUrl, LogoUrl: bfsResult.LogoUrl,
                    ExtractedName: bfsResult.ExtractedName, ExtractedDate: bfsResult.StartPageDate,
                    StartFee: bfsResult.StartFee, Currency: bfsResult.Currency);
            }

            return new RaceScraperResult([], ImageUrl: bfsResult.ImageUrl, LogoUrl: bfsResult.LogoUrl,
                ExtractedName: bfsResult.ExtractedName, ExtractedDate: bfsResult.StartPageDate,
                StartFee: bfsResult.StartFee, Currency: bfsResult.Currency);
        }

        var baseName = eventName;
        var routeDistancesKm = bfsResult.Routes.Select(r => GpxParser.CalculateDistanceKm(r.Route.Coordinates)).ToList();
        var distanceAssignments = RaceScrapeDiscovery.AssignDistancesToRoutes(routeDistancesKm, job.Distance);

        var routes = new List<ScrapedRoute>(bfsResult.Routes.Count);
        for (int i = 0; i < bfsResult.Routes.Count; i++)
        {
            var (parsedRoute, gpxUrl, routeDate, routeElevation, routeImage) = bfsResult.Routes[i];
            var gpxDistanceKm = routeDistancesKm[i];
            var assignedDistances = distanceAssignments[i];

            string? routeName = null;
            if (bfsResult.Routes.Count > 1)
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

            // Use the date from the page where this specific GPX was found, falling back to start page.
            var date = routeDate ?? bfsResult.StartPageDate;

            // For single-route events, fall back to start page elevation when the GPX
            // source page didn't have its own value (all info may be on the landing page).
            var elevation = routeElevation ?? (bfsResult.Routes.Count == 1 ? bfsResult.StartPageElevation : null);

            routes.Add(new ScrapedRoute(
                Coordinates: parsedRoute.Coordinates,
                SourceUrl: startUrl,
                Name: routeName,
                Distance: routeDistance,
                ElevationGain: elevation,
                GpxUrl: gpxUrl,
                ImageUrl: routeImage ?? bfsResult.ImageUrl,
                LogoUrl: bfsResult.LogoUrl,
                Date: date,
                StartFee: bfsResult.StartFee,
                Currency: bfsResult.Currency));
        }

        var scrapedDate = bfsResult.Routes.Select(r => r.Date).FirstOrDefault(d => d is not null) ?? bfsResult.StartPageDate;
        return new RaceScraperResult(routes, ImageUrl: bfsResult.ImageUrl, LogoUrl: bfsResult.LogoUrl,
            ExtractedName: bfsResult.ExtractedName, ExtractedDate: scrapedDate,
            StartFee: bfsResult.StartFee, Currency: bfsResult.Currency);
    }

    private record BfsResult(
        IReadOnlyList<(ParsedGpxRoute Route, Uri GpxUrl, string? Date, double? ElevationGain, Uri? ImageUrl)> Routes,
        Uri? ImageUrl,
        Uri? LogoUrl,
        string? StartPageDate,
        // Course pages found by URL pattern (distance in path) — used when no GPX found.
        IReadOnlyList<CoursePage> CoursePages,
        // Event name extracted from headings/title/meta across pages.
        string? ExtractedName,
        string? StartFee = null,
        string? Currency = null,
        double? StartPageElevation = null);

    private record CoursePage(Uri Url, string Html, string? Distance, bool IsContentOnly = false);

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
        CancellationToken cancellationToken)
    {
        var visitedPages = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        // Maps GPX URL → source page info (first page that referenced the GPX wins).
        var gpxUrlToPage = new Dictionary<string, (string Html, Uri PageUrl)>(StringComparer.OrdinalIgnoreCase);
        // Track HTML content for pages that had GPX links (for image extraction).
        var pagesWithGpx = new List<(Uri PageUri, string Html)>();
        // Track pages reached via course links that have a distance in their URL.
        var coursePages = new List<CoursePage>();
        var coursePageUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        // Track external "GPX"-labelled links to probe after the BFS loop.
        // Maps external URL → (source page HTML, source page URL) so findings are attributed to the internal page.
        var externalGpxProbes = new Dictionary<string, (string Html, Uri PageUrl)>(StringComparer.OrdinalIgnoreCase);
        // Google Drive folder URLs found during BFS — resolved to download links after the BFS loop.
        var driveFolderProbes = new Dictionary<string, (string Html, Uri PageUrl)>(StringComparer.OrdinalIgnoreCase);
        string? startPageHtml = null;

        // Process one depth level at a time, fetching all pages in the level concurrently.
        var currentLevel = new List<(Uri Url, bool IsCourseLink)> { (startUrl, false) };

        for (int depth = 0; depth <= MaxDepth && currentLevel.Count > 0; depth++)
        {
            var pagesToFetch = new List<(Uri Url, bool IsCourseLink)>();
            foreach (var entry in currentLevel)
            {
                if (visitedPages.Count >= MaxPages) break;
                if (visitedPages.Add(StripFragment(entry.Url)))
                    pagesToFetch.Add(entry);
            }

            if (pagesToFetch.Count == 0) break;

            logger.LogInformation("BFS depth {Depth}: fetching {Count} pages", depth, pagesToFetch.Count);
            foreach (var p in pagesToFetch)
                logger.LogDebug("BFS depth {Depth}: {Url}", depth, p.Url);

            var fetchTasks = pagesToFetch.Select(e => TryFetchStringAsync(httpClient, e.Url, cancellationToken));
            var htmlResults = await Task.WhenAll(fetchTasks);
            logger.LogInformation("BFS depth {Depth}: fetched {Ok}/{Total} pages", depth, htmlResults.Count(r => r is not null), htmlResults.Length);

            var nextLevel = new List<(Uri Url, bool IsCourseLink)>();
            for (int i = 0; i < pagesToFetch.Count; i++)
            {
                var html = htmlResults[i];
                if (html is null) continue;

                var pageUrl = pagesToFetch[i].Url;
                var isCourseLink = pagesToFetch[i].IsCourseLink;

                if (pageUrl.AbsoluteUri.Equals(startUrl.AbsoluteUri, StringComparison.OrdinalIgnoreCase))
                    startPageHtml = html;

                var pageGpxLinks = RaceHtmlScraper.ExtractGpxLinksFromHtml(html, pageUrl);
                if (pageGpxLinks.Count > 0)
                    pagesWithGpx.Add((pageUrl, html));
                foreach (var gpxLink in pageGpxLinks)
                {
                    if (IsGoogleDriveFolder(gpxLink))
                    {
                        driveFolderProbes.TryAdd(gpxLink.AbsoluteUri, (html, pageUrl));
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

                // Track as course page if URL contains a distance pattern (e.g. "/80-km"),
                // Track as course page if URL contains a distance pattern (e.g. "/80-km"),
                // OR if a sub-page's visible content mentions km distances AND has a date.
                // Content-distance extraction skips the start page (depth 0) — it lists all
                // distances but isn't a single-race page. For sub-pages, use the max distance
                // found so navigation/context references to smaller distances are ignored.
                if (!coursePageUrls.Contains(StripFragment(pageUrl)))
                {
                    var urlDistance = RaceHtmlScraper.ExtractDistanceFromUrl(pageUrl);
                    if (urlDistance is not null)
                    {
                        coursePageUrls.Add(StripFragment(pageUrl));
                        coursePages.Add(new CoursePage(pageUrl, html, urlDistance));
                    }
                    else if (depth > 0)
                    {
                        var contentDistances = RaceHtmlScraper.ExtractDistancesFromContent(html);
                        if (contentDistances.Count > 0 && RaceHtmlScraper.ExtractDate(html) is not null)
                        {
                            coursePageUrls.Add(StripFragment(pageUrl));
                            // Use the largest distance — sub-pages typically describe one race
                            // and smaller km mentions are navigation or context references.
                            var maxDist = contentDistances
                                .OrderByDescending(d =>
                                {
                                    var m = System.Text.RegularExpressions.Regex.Match(d, @"[\d.]+");
                                    return m.Success && double.TryParse(m.Value,
                                        System.Globalization.NumberStyles.Float,
                                        System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : 0;
                                })
                                .First();
                            coursePages.Add(new CoursePage(pageUrl, html, maxDist, IsContentOnly: true));
                        }
                    }
                }

                // Pick up Google Drive folder links from download-text anchors on the page.
                foreach (var dlLink in RaceHtmlScraper.ExtractDownloadLinksFromHtml(html, pageUrl))
                {
                    if (IsGoogleDriveFolder(dlLink))
                        driveFolderProbes.TryAdd(dlLink.AbsoluteUri, (html, pageUrl));
                }

                if (depth < MaxDepth)
                {
                    foreach (var courseLink in RaceHtmlScraper.ExtractCourseLinksFromHtml(html, pageUrl))
                    {
                        if (!visitedPages.Contains(StripFragment(courseLink))
                            && IsSameDomain(courseLink, startUrl))
                            nextLevel.Add((courseLink, true));
                    }
                }
            }

            currentLevel = nextLevel;
        }

        logger.LogInformation("BFS done. Visited {Pages} pages, found {Gpx} GPX URLs, {ExtProbes} external probes",
            visitedPages.Count, gpxUrlToPage.Count, externalGpxProbes.Count);

        // Probe external "GPX"-labelled links: fetch the page and scrape it for GPX files.
        // Any GPX found is attributed to the internal page that linked to the external site.
        foreach (var (externalUrl, (sourceHtml, sourcePageUrl)) in externalGpxProbes.Take(5))
        {
            logger.LogDebug("BFS: probing external GPX link {Url}", externalUrl);
            if (gpxUrlToPage.ContainsKey(externalUrl)) continue;

            var externalContent = await TryFetchStringAsync(httpClient, new Uri(externalUrl), cancellationToken);
            if (externalContent is null) continue;

            // If the external URL returned GPX directly, register it with the internal source page.
            if (GpxParser.TryParseRoute(externalContent) is not null)
            {
                gpxUrlToPage.TryAdd(externalUrl, (sourceHtml, sourcePageUrl));
                continue;
            }

            // Otherwise treat the content as HTML and look for GPX/download links within it.
            var probeUri = new Uri(externalUrl);
            var innerGpxLinks = RaceHtmlScraper.ExtractGpxLinksFromHtml(externalContent, probeUri)
                .Concat(RaceHtmlScraper.ExtractDownloadLinksFromHtml(externalContent, probeUri))
                .Take(10);

            foreach (var innerLink in innerGpxLinks)
                gpxUrlToPage.TryAdd(innerLink.AbsoluteUri, (sourceHtml, sourcePageUrl));
        }

        logger.LogInformation("BFS: external probes done. Total GPX URLs: {Count}", gpxUrlToPage.Count);

        // Probe Google Drive folder links: fetch the folder page, extract file IDs from data-id attributes,
        // and construct download URLs. Attributed to the internal page that linked to the folder.
        foreach (var (folderUrl, (sourceHtml, sourcePageUrl)) in driveFolderProbes.Take(5))
        {
            logger.LogDebug("BFS: probing Google Drive folder {Url}", folderUrl);
            var folderHtml = await TryFetchStringAsync(httpClient, new Uri(folderUrl), cancellationToken);
            if (folderHtml is null) continue;

            var downloadUrls = ExtractGoogleDriveDownloadUrls(folderHtml);
            logger.LogInformation("BFS: Drive folder yielded {Count} download URLs", downloadUrls.Count);
            foreach (var dlUrl in downloadUrls)
                gpxUrlToPage.TryAdd(dlUrl.AbsoluteUri, (sourceHtml, sourcePageUrl));
        }

        if (driveFolderProbes.Count > 0)
            logger.LogInformation("BFS: Drive folder probes done. Total GPX URLs: {Count}", gpxUrlToPage.Count);

        // Fetch external CSS stylesheets from the start page so background-image URLs are found by image extraction.
        string? cssContent = null;
        if (startPageHtml is not null)
        {
            var cssUrls = RaceHtmlScraper.ExtractStylesheetUrls(startPageHtml, startUrl);
            logger.LogInformation("BFS: found {Count} CSS stylesheets to fetch", cssUrls.Count);
            if (cssUrls.Count > 0)
            {
                var cssTasks = cssUrls.Take(5).Select(u => TryFetchStringAsync(httpClient, u, cancellationToken));
                var cssResults = await Task.WhenAll(cssTasks);
                cssContent = string.Join("\n", cssResults.Where(c => c is not null));
                logger.LogInformation("BFS: CSS fetched, total {Len} chars", cssContent.Length);
            }
        }

        logger.LogInformation("BFS: extracting image from {GpxPages} gpx pages, {CoursePages} course pages ({ContentOnly} content-only)",
            pagesWithGpx.Count, coursePages.Count, coursePages.Count(cp => cp.IsContentOnly));

        // Extract image: prefer the start page (landing page) first, then fall back to
        // course pages and GPX pages only when the start page has no image.
        // Append fetched CSS to each HTML so background-image URLs in stylesheets are discovered.
        Uri? imageUrl = null;
        if (startPageHtml is not null)
        {
            var combined = cssContent is not null ? startPageHtml + "\n" + cssContent : startPageHtml;
            imageUrl = RaceHtmlScraper.ExtractProminentImage(combined, startUrl);
        }
        if (imageUrl is null)
        {
            foreach (var cp in coursePages)
            {
                var combined = cssContent is not null ? cp.Html + "\n" + cssContent : cp.Html;
                imageUrl = RaceHtmlScraper.ExtractProminentImage(combined, cp.Url);
                if (imageUrl is not null) break;
            }
        }
        if (imageUrl is null)
        {
            foreach (var (pageUri, html) in pagesWithGpx)
            {
                var combined = cssContent is not null ? html + "\n" + cssContent : html;
                imageUrl = RaceHtmlScraper.ExtractProminentImage(combined, pageUri);
                if (imageUrl is not null) break;
            }
        }

        logger.LogInformation("BFS: image done, extracting logo");
        // Extract logo from the start page.
        var logoUrl = startPageHtml is not null ? RaceHtmlScraper.ExtractLogo(startPageHtml, startUrl) : null;

        logger.LogInformation("BFS: logo done, extracting date");
        var startPageDate = startPageHtml is not null ? RaceHtmlScraper.ExtractDate(startPageHtml) : null;

        logger.LogInformation("BFS: date done, extracting name");
        // Extract event name from headings/title/meta across start page and course pages.
        var courseHtmls = coursePages.Select(cp => (cp.Url, cp.Html)).ToList();
        var extractedName = RaceHtmlScraper.ExtractEventName(startUrl, startPageHtml, courseHtmls);

        logger.LogInformation("BFS: name done, extracting elevation & price");
        var startPageElevation = startPageHtml is not null ? RaceHtmlScraper.ExtractElevationGain(startPageHtml) : null;
        var price = startPageHtml is not null ? RaceHtmlScraper.ExtractPrice(startPageHtml, startUrl) : null;
        var startFee = price?.Amount.ToString();
        var currency = price?.Currency;

        logger.LogInformation("BFS: image={Image}, logo={Logo}, date={Date}, name={Name}, elevation={Elevation}, price={Fee} {Currency}, gpxUrls={GpxCount}",
            imageUrl, logoUrl, startPageDate, extractedName, startPageElevation, startFee, currency, gpxUrlToPage.Count);

        if (gpxUrlToPage.Count == 0)
            return new BfsResult([], imageUrl, logoUrl, startPageDate, coursePages, extractedName, startFee, currency, startPageElevation);

        // Extract per-GPX dates, elevation gain, and images from the source page where each GPX link was found.
        var gpxUrlDates = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        var gpxUrlElevation = new Dictionary<string, double?>(StringComparer.OrdinalIgnoreCase);
        var gpxUrlImages = new Dictionary<string, Uri?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (gpxUrl, (html, pageUrl)) in gpxUrlToPage)
        {
            gpxUrlDates[gpxUrl] = RaceHtmlScraper.ExtractDate(html);
            gpxUrlElevation[gpxUrl] = RaceHtmlScraper.ExtractElevationGain(html);
            var combined = cssContent is not null ? html + "\n" + cssContent : html;
            gpxUrlImages[gpxUrl] = RaceHtmlScraper.ExtractProminentImage(combined, pageUrl);
        }

        // Fetch all GPX URLs concurrently.
        logger.LogInformation("BFS: fetching {Count} GPX URLs", gpxUrlToPage.Count);
        foreach (var url in gpxUrlToPage.Keys)
            logger.LogDebug("BFS: GPX URL {Url}", url);
        var gpxTasks = gpxUrlToPage.Keys.Select(url => TryFetchGpxFromUrlAsync(httpClient, new Uri(url), cancellationToken));
        var gpxResults = await Task.WhenAll(gpxTasks);
        logger.LogInformation("BFS: fetched GPX, {Parsed} parsed successfully", gpxResults.Count(r => r.HasValue));

        var routes = gpxResults
            .Where(r => r.HasValue)
            .Select(r =>
            {
                var (route, gpxUrl) = r!.Value;
                var date = gpxUrlDates.GetValueOrDefault(gpxUrl.AbsoluteUri);
                var elevation = gpxUrlElevation.GetValueOrDefault(gpxUrl.AbsoluteUri);
                var routeImage = gpxUrlImages.GetValueOrDefault(gpxUrl.AbsoluteUri);
                return (route, gpxUrl, Date: date, ElevationGain: elevation, ImageUrl: routeImage);
            })
            .ToList();

        // Deduplicate: keep one route per distance (rounded to nearest km).
        // On collision prefer the most complete entry (GPX coordinates count, then metadata fields), then first found.
        var deduped = routes
            .GroupBy(r => (int)Math.Round(GpxParser.CalculateDistanceKm(r.route.Coordinates)))
            .Select(g => g
                .OrderByDescending(r => r.route.Coordinates.Count)
                .ThenByDescending(r =>
                    (r.Date is not null ? 1 : 0) +
                    (r.ElevationGain is not null ? 1 : 0) +
                    (r.ImageUrl is not null ? 1 : 0))
                .First())
            .ToList();

        logger.LogInformation("BFS: returning {Routes} routes (deduped from {Raw})", deduped.Count, routes.Count);
        return new BfsResult(routes, imageUrl, logoUrl, startPageDate, coursePages, extractedName, startFee, currency, startPageElevation);
    }

    private async Task<(ParsedGpxRoute Route, Uri GpxUrl)?> TryFetchGpxFromUrlAsync(
        HttpClient httpClient,
        Uri url,
        CancellationToken cancellationToken)
    {
        var content = await TryFetchStringAsync(httpClient, url, cancellationToken);
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
            var linkContent = await TryFetchStringAsync(httpClient, link, cancellationToken);
            if (linkContent is null) return ((ParsedGpxRoute, Uri)?)null;
            var linkParsed = GpxParser.TryParseRoute(linkContent);
            return linkParsed is not null ? (linkParsed, link) : null;
        });

        var secondaryResults = await Task.WhenAll(secondaryTasks);
        return secondaryResults.FirstOrDefault(r => r.HasValue);
    }

    private const int MaxResponseBytes = 2 * 1024 * 1024; // 2 MB

    private async Task<string?> TryFetchStringAsync(HttpClient httpClient, Uri url, CancellationToken cancellationToken)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
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

            // Reject if Content-Length exceeds limit.
            if (response.Content.Headers.ContentLength > MaxResponseBytes)
                return null;

            var bytes = await response.Content.ReadAsByteArrayAsync(cts.Token);
            if (bytes.Length > MaxResponseBytes) return null;
            return System.Text.Encoding.UTF8.GetString(bytes);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw; // Genuine cancellation — propagate.
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "BfsScraper: could not fetch {Url}", url);
            return null;
        }
    }

    /// <summary>Checks whether a URL points to a Google Drive folder.</summary>
    private static bool IsGoogleDriveFolder(Uri uri) =>
        uri.Host.Equals("drive.google.com", StringComparison.OrdinalIgnoreCase)
        && uri.AbsolutePath.StartsWith("/drive/folders/", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Extracts download URLs from a Google Drive folder page HTML.
    /// Looks for data-id attributes containing file IDs and constructs direct download URLs.
    /// </summary>
    private static List<Uri> ExtractGoogleDriveDownloadUrls(string folderHtml)
    {
        var results = new List<Uri>();
        // data-id="FILE_ID" — each file in the folder listing has one.
        foreach (System.Text.RegularExpressions.Match m in
            System.Text.RegularExpressions.Regex.Matches(folderHtml, @"data-id=""(?<id>[0-9A-Za-z_-]{20,})"""))
        {
            var fileId = m.Groups["id"].Value;
            var downloadUrl = new Uri($"https://drive.google.com/uc?export=download&id={fileId}");
            results.Add(downloadUrl);
        }
        return results.DistinctBy(u => u.AbsoluteUri).ToList();
    }
}
