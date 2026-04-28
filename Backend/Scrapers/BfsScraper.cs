using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend.Scrapers;

// Performs a breadth-first search from a race website URL to discover GPX route files.
// Depth 3 / max 30 pages.
internal sealed class BfsScraper(ILogger logger)
{
    private const int MaxDepth = 3;
    private const int MaxPages = 30;

    /// <summary>
    /// BFS-scrapes each URL in <paramref name="urls"/> and merges the results.
    /// </summary>
    public async Task<RaceScraperResult?> ScrapeAsync(
        IReadOnlyList<Uri> urls,
        HttpClient httpClient,
        CancellationToken cancellationToken)
    {
        if (urls.Count == 0) return null;

        var filtered = urls.Where(u => !IsBareSocialDomain(u)).ToList();
        if (filtered.Count == 0) return null;

        return await ScrapeSiteAsync(filtered[0], filtered, httpClient, cancellationToken);
    }

    private async Task<RaceScraperResult?> ScrapeSiteAsync(
        Uri startUrl,
        IReadOnlyList<Uri> scrapeUrls,
        HttpClient httpClient,
        CancellationToken cancellationToken)
    {
        var bfsResult = await FindGpxRoutesAsync(httpClient, startUrl, scrapeUrls, cancellationToken);
        var (imageUrl, logoUrl, startPageDate, _, startFee, currency, _, _, _) =
            ExtractCoursePageMetadata(bfsResult.StartPageHtml ?? string.Empty, startUrl, bfsResult.CssContent);
        var extractedName = RaceHtmlScraper.ExtractEventName(startUrl, bfsResult.StartPageHtml,
            [.. bfsResult.CoursePages.Select(cp => (cp.Url, cp.Html))]);

        if (bfsResult.Routes.Count == 0)
        {
            // No GPX found — return course pages as coordinate-less routes if available.
            if (bfsResult.CoursePages.Count > 0)
            {
                var fallbackDate = startPageDate;

                // Only keep course pages from the same domain as the startUrl
                var sameDomainCoursePages = bfsResult.CoursePages
                    .Where(cp => IsSameDomain(cp.Url, startUrl))
                    .ToList();

                // Deduplicate by rounded distance: prefer URL-distance pages over content-only ones,
                // then prefer entries with more metadata.
                var dedupedCoursePages = sameDomainCoursePages
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
                        : [g.OrderBy(cp => cp.IsContentOnly ? 1 : 0).First()])
                    .OrderBy(cp => cp.IsContentOnly ? 1 : 0)
                    .ToList();

                var courses = dedupedCoursePages.Select(cp =>
                {
                    var (ImageUrl, LogoUrl, Date, ElevationGain, StartFee, Currency, RaceType, Name, _) = ExtractCoursePageMetadata(cp.Html, cp.Url, null, bfsResult.StartPageHtml);
                    var name = Name ?? "Unnamed";
                    var distanceStr = cp.Distance;
                    return new ScrapedRoute(
                        Coordinates: [],
                        SourceUrl: cp.Url,
                        Name: name,
                        Distance: distanceStr,
                        ElevationGain: ElevationGain,
                        ImageUrl: ImageUrl,
                        LogoUrl: LogoUrl,
                        Date: Date,
                        StartFee: StartFee,
                        Currency: Currency,
                        RaceType: RaceType);
                }).ToList();

                return new RaceScraperResult(courses, ImageUrl: imageUrl, LogoUrl: logoUrl,
                    ExtractedName: extractedName, ExtractedDate: startPageDate,
                    StartFee: startFee, Currency: currency);
            }

            return new RaceScraperResult([], ImageUrl: imageUrl, LogoUrl: logoUrl,
                ExtractedName: extractedName, ExtractedDate: startPageDate,
                StartFee: startFee, Currency: currency);
        }


        // For GPX routes, extract name and distance from the course page HTML if possible
        var routeDistancesKm = bfsResult.Routes.Select(r => GpxParser.CalculateDistanceKm(r.Route.Coordinates)).ToList();
        var routes = new List<ScrapedRoute>(bfsResult.Routes.Count);
        for (int i = 0; i < bfsResult.Routes.Count; i++)
        {
            var (parsedRoute, gpxUrl, sourcePageUrl, sourceHtml) = bfsResult.Routes[i];
            if (!IsSameDomain(sourcePageUrl, startUrl))
                continue;

            var (img, logo, date, elevationGain, startingFee, curr, raceType, routeName, distanceStr) = ExtractCoursePageMetadata(sourceHtml ?? string.Empty, sourcePageUrl, null, bfsResult.StartPageHtml);
            var name = routeName ?? "Unnamed";
            routes.Add(new ScrapedRoute(
                Coordinates: parsedRoute.Coordinates,
                SourceUrl: sourcePageUrl,
                Name: name,
                Distance: distanceStr,
                ElevationGain: elevationGain,
                GpxUrl: gpxUrl,
                ImageUrl: img,
                LogoUrl: logo,
                Date: date,
                StartFee: startingFee,
                Currency: curr,
                GpxSource: GpxSourceResolver.Resolve(gpxUrl, startUrl),
                RaceType: raceType));
        }

        return new RaceScraperResult(routes, ImageUrl: imageUrl, LogoUrl: logoUrl,
            ExtractedName: extractedName, ExtractedDate: routes[0].Date,
            StartFee: startFee, Currency: currency);
    }

    private record BfsResult(
        IReadOnlyList<(ParsedGpxRoute Route, Uri GpxUrl, Uri SourcePageUrl, string? SourceHtml)> Routes,
        string? StartPageHtml,
        IReadOnlyList<CoursePage> CoursePages,
        string? CssContent);

    private record CoursePage(Uri Url, string Html, string? Distance, bool IsContentOnly = false);

    private static (Uri? ImageUrl, Uri? LogoUrl, string? Date, double? ElevationGain, string? StartFee, string? Currency, string? RaceType, string? Name, string? Distance) ExtractCoursePageMetadata(
        string html,
        Uri pageUrl,
        string? cssContent,
        string? startPageHtml = null)
    {
        var combined = cssContent is not null ? html + "\n" + cssContent : html;
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
        var distance = RaceHtmlScraper.ExtractDistanceFromUrl(pageUrl) ??
                       RaceHtmlScraper.ExtractDistancesFromContent(html).FirstOrDefault();

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
        var inlineGpxByUrl = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        string? startPageHtml = null;

        // Process one depth level at a time, fetching all pages in the level concurrently.
        var currentLevel = scrapeUrls.Select(u => (Url: u, IsCourseLink: false)).ToList();

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

            logger.LogDebug("BFS depth {Depth}: fetching {Count} pages", depth, pagesToFetch.Count);
            foreach (var p in pagesToFetch)
                logger.LogDebug("BFS depth {Depth}: {Url}", depth, p.Url);

            var fetchTasks = pagesToFetch.Select(e => TryFetchStringAsync(httpClient, e.Url, cancellationToken));
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

                var pageGpxLinks = RaceHtmlScraper.ExtractGpxLinksFromHtml(html, pageUrl);
                foreach (var gpxLink in pageGpxLinks)
                {
                    if (IsGoogleDriveFolder(gpxLink))
                    {
                        driveFolderProbes.TryAdd(gpxLink.AbsoluteUri, (html, pageUrl));
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

                // Pick up Google Drive folder links from download-text anchors on the page.
                foreach (var dlLink in RaceHtmlScraper.ExtractDownloadLinksFromHtml(html, pageUrl))
                {
                    if (IsGoogleDriveFolder(dlLink))
                        driveFolderProbes.TryAdd(dlLink.AbsoluteUri, (html, pageUrl));
                    else if (DropboxShareParser.IsDropboxSharedFolder(dlLink))
                        dropboxFolderProbes.TryAdd(dlLink.AbsoluteUri, (html, pageUrl));
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

        logger.LogDebug("BFS crawl done. Visited {Pages} pages, found {Gpx} GPX URLs, {ExtProbes} external probes",
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

        logger.LogDebug("BFS: external probes done. Total GPX URLs: {Count}", gpxUrlToPage.Count);

        // Probe Google Drive folder links: recursively crawl subfolders (skipping ignored names)
        // and collect download URLs. Attributed to the internal page that linked to the folder.
        foreach (var (folderUrl, (sourceHtml, sourcePageUrl)) in driveFolderProbes.Take(5))
        {
            logger.LogDebug("BFS: probing Google Drive folder {Url}", folderUrl);
            var downloadUrls = await CrawlGoogleDriveFolderAsync(httpClient, new Uri(folderUrl), cancellationToken);
            logger.LogDebug("BFS: Drive folder yielded {Count} download URLs", downloadUrls.Count);
            foreach (var dlUrl in downloadUrls)
                gpxUrlToPage.TryAdd(dlUrl.AbsoluteUri, (sourceHtml, sourcePageUrl));
        }

        if (driveFolderProbes.Count > 0)
            logger.LogDebug("BFS: Drive folder probes done. Total GPX URLs: {Count}", gpxUrlToPage.Count);

        // Dropbox shared folders: dl=1 returns a zip; extract .gpx files and register clickable folder URLs + fragment per entry.
        foreach (var (folderUrl, (sourceHtml, sourcePageUrl)) in dropboxFolderProbes.Take(5))
        {
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

        // Fetch external CSS stylesheets from the start page so background-image URLs are found by image extraction.
        string? cssContent = null;
        if (startPageHtml is not null)
        {
            var cssUrls = RaceHtmlScraper.ExtractStylesheetUrls(startPageHtml, startUrl);
            logger.LogDebug("BFS: found {Count} CSS stylesheets to fetch", cssUrls.Count);
            if (cssUrls.Count > 0)
            {
                var cssTasks = cssUrls.Take(5).Select(u => TryFetchStringAsync(httpClient, u, cancellationToken));
                var cssResults = await Task.WhenAll(cssTasks);
                cssContent = string.Join("\n", cssResults.Where(c => c is not null));
                logger.LogDebug("BFS: CSS fetched, total {Len} chars", cssContent.Length);
            }
        }

        if (gpxUrlToPage.Count == 0)
            return new BfsResult([], startPageHtml, coursePages, cssContent);

        // Fetch all GPX URLs concurrently.
        var gpxTasks = gpxUrlToPage.Keys.Select(url =>
            TryFetchGpxFromUrlAsync(httpClient, new Uri(url), cancellationToken, inlineGpxByUrl));
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
            .ToList();

        logger.LogInformation("BFS: {Url} — visited {Pages} pages, {GpxFound} GPX parsed, {Routes} routes",
            startUrl, visitedPages.Count, parsedCount, routes.Count);
        return new BfsResult(routes, startPageHtml, coursePages, cssContent);
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

        var content = await TryFetchStringAsync(httpClient, fetchUrl, cancellationToken, inlineGpxByUrl);
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
            var linkContent = await TryFetchStringAsync(httpClient, link, cancellationToken, inlineGpxByUrl);
            if (linkContent is null) return ((ParsedGpxRoute, Uri)?)null;
            var linkParsed = GpxParser.TryParseRoute(linkContent);
            return linkParsed is not null ? (linkParsed, link) : null;
        });

        var secondaryResults = await Task.WhenAll(secondaryTasks);
        return secondaryResults.FirstOrDefault(r => r.HasValue);
    }

    private const int MaxResponseBytes = 2 * 1024 * 1024; // 2 MB

    private async Task<string?> TryFetchStringAsync(
        HttpClient httpClient,
        Uri url,
        CancellationToken cancellationToken,
        IReadOnlyDictionary<string, string>? inlineGpxByUrl = null)
    {
        if (inlineGpxByUrl?.TryGetValue(url.AbsoluteUri, out var inline) == true)
            return inline;

        var fetchUri = DropboxShareParser.IsDropboxHost(url) && DropboxShareParser.IsDropboxSharedFile(url)
            ? DropboxShareParser.WithDl1(url)
            : url;

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));
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

            // Reject if Content-Length exceeds limit.
            if (response.Content.Headers.ContentLength > MaxResponseBytes)
                return null;

            var bytes = await response.Content.ReadAsByteArrayAsync(cts.Token);
            if (bytes.Length > MaxResponseBytes) return null;
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
        CancellationToken cancellationToken,
        HashSet<string>? visited = null,
        int depth = 0,
        string? preloadedHtml = null)
    {
        const int MaxDriveDepth = 3;
        visited ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (depth > MaxDriveDepth || !visited.Add(folderUrl.AbsoluteUri))
            return [];

        logger.LogDebug("BFS: crawling Drive folder depth={Depth} {Url}", depth, folderUrl);
        var folderHtml = preloadedHtml ?? await TryFetchStringAsync(httpClient, folderUrl, cancellationToken);
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
            var html = await TryFetchStringAsync(httpClient, candidateUri, cancellationToken);
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
                    httpClient, subfolderUri, cancellationToken, visited, depth + 1, itemHtml);
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

    /// <summary>Checks whether a URL points to a Google Drive folder.</summary>
    private static bool IsGoogleDriveFolder(Uri uri) =>
        uri.Host.Equals("drive.google.com", StringComparison.OrdinalIgnoreCase)
        && uri.AbsolutePath.StartsWith("/drive/folders/", StringComparison.OrdinalIgnoreCase);

    /// <summary>Checks that both URIs share the same registrable domain (ignoring www prefix).</summary>
    private static bool IsSameDomain(Uri candidate, Uri origin)
    {
        static string NormalizeHost(string host) =>
            host.StartsWith("www.", StringComparison.OrdinalIgnoreCase) ? host[4..] : host;

        return NormalizeHost(candidate.Host).Equals(NormalizeHost(origin.Host), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Social/platform domains where a bare URL (no slug) is useless to BFS-crawl.
    /// URLs with a meaningful path (e.g. facebook.com/events/123) are still allowed.
    /// </summary>
    private static readonly string[] SocialDomains =
        ["facebook.com", "fb.me", "fb.com", "youtube.com", "youtu.be",
         "instagram.com", "twitter.com", "x.com", "tiktok.com", "linkedin.com"];

    private static bool IsBareSocialDomain(Uri uri)
    {
        var host = uri.Host.ToLowerInvariant();
        if (host.StartsWith("www.")) host = host[4..];
        if (!SocialDomains.Any(d => host.Equals(d, StringComparison.OrdinalIgnoreCase)))
            return false;
        var path = uri.AbsolutePath.TrimEnd('/');
        return string.IsNullOrEmpty(path);
    }
}
