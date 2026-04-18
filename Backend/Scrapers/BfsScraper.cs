using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend.Scrapers;

// Performs a breadth-first search from a race website URL to discover GPX route files.
// Depth 3 / max 30 pages.
internal sealed class BfsScraper(ILogger logger) : IRaceScraper
{
    private const int MaxDepth = 3;
    private const int MaxPages = 30;

    private static readonly HashSet<string> BlacklistedDomains = new(StringComparer.OrdinalIgnoreCase)
    {
        "facebook.com", "fb.com", "instagram.com", "twitter.com", "x.com",
        "linkedin.com", "youtube.com", "youtu.be", "tiktok.com",
        "pinterest.com", "reddit.com", "threads.net",
        "google.com", "maps.google.com", "goo.gl",
        "apple.com", "apps.apple.com", "play.google.com",
        "wa.me", "t.me", "discord.gg", "outlook.com", "mail.google.com",
        "flickr.com", "vimeo.com", "dailymotion.com",
    };

    private static bool IsBlacklisted(Uri uri)
    {
        var host = uri.Host;
        foreach (var domain in BlacklistedDomains)
        {
            if (host.Equals(domain, StringComparison.OrdinalIgnoreCase)
                || host.EndsWith("." + domain, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
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
                var courses = bfsResult.CoursePages.Select(cp =>
                {
                    var pageImage = RaceHtmlScraper.ExtractProminentImage(cp.Html, cp.Url);
                    var pageDate = RaceHtmlScraper.ExtractDate(cp.Html) ?? fallbackDate;
                    var pageElevation = RaceHtmlScraper.ExtractElevationGain(cp.Html);
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
                        Date: pageDate);
                }).ToList();

                return new RaceScraperResult(courses, ImageUrl: bfsResult.ImageUrl, LogoUrl: bfsResult.LogoUrl,
                    ExtractedName: bfsResult.ExtractedName, ExtractedDate: bfsResult.StartPageDate);
            }

            return new RaceScraperResult([], ImageUrl: bfsResult.ImageUrl, LogoUrl: bfsResult.LogoUrl,
                ExtractedName: bfsResult.ExtractedName, ExtractedDate: bfsResult.StartPageDate);
        }

        var baseName = eventName;
        var routeDistancesKm = bfsResult.Routes.Select(r => GpxParser.CalculateDistanceKm(r.Route.Coordinates)).ToList();
        var distanceAssignments = RaceScrapeDiscovery.AssignDistancesToRoutes(routeDistancesKm, job.Distance);

        var routes = new List<ScrapedRoute>(bfsResult.Routes.Count);
        for (int i = 0; i < bfsResult.Routes.Count; i++)
        {
            var (parsedRoute, gpxUrl, routeDate, routeElevation) = bfsResult.Routes[i];
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

            routes.Add(new ScrapedRoute(
                Coordinates: parsedRoute.Coordinates,
                SourceUrl: startUrl,
                Name: routeName,
                Distance: routeDistance,
                ElevationGain: routeElevation,
                GpxUrl: gpxUrl,
                ImageUrl: bfsResult.ImageUrl,
                LogoUrl: bfsResult.LogoUrl,
                Date: date));
        }

        var scrapedDate = bfsResult.Routes.Select(r => r.Date).FirstOrDefault(d => d is not null) ?? bfsResult.StartPageDate;
        return new RaceScraperResult(routes, ImageUrl: bfsResult.ImageUrl, LogoUrl: bfsResult.LogoUrl,
            ExtractedName: bfsResult.ExtractedName, ExtractedDate: scrapedDate);
    }

    private record BfsResult(
        IReadOnlyList<(ParsedGpxRoute Route, Uri GpxUrl, string? Date, double? ElevationGain)> Routes,
        Uri? ImageUrl,
        Uri? LogoUrl,
        string? StartPageDate,
        // Course pages found by URL pattern (distance in path) — used when no GPX found.
        IReadOnlyList<CoursePage> CoursePages,
        // Event name extracted from headings/title/meta across pages.
        string? ExtractedName);

    private record CoursePage(Uri Url, string Html, string? Distance);

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
        // Maps GPX URL → source page HTML (first page that referenced the GPX wins).
        var gpxUrlToPageHtml = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        // Track HTML content for pages that had GPX links (for image extraction).
        var pagesWithGpx = new List<(Uri PageUri, string Html)>();
        // Track pages reached via course links that have a distance in their URL.
        var coursePages = new List<CoursePage>();
        var coursePageUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
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

            var fetchTasks = pagesToFetch.Select(e => TryFetchStringAsync(httpClient, e.Url, cancellationToken));
            var htmlResults = await Task.WhenAll(fetchTasks);

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
                    gpxUrlToPageHtml.TryAdd(gpxLink.AbsoluteUri, html);

                // Track as course page if reached via a course link and has distance in URL.
                if (isCourseLink && coursePageUrls.Add(StripFragment(pageUrl)))
                {
                    var urlDistance = RaceHtmlScraper.ExtractDistanceFromUrl(pageUrl);
                    if (urlDistance is not null)
                        coursePages.Add(new CoursePage(pageUrl, html, urlDistance));
                }

                if (depth < MaxDepth)
                {
                    foreach (var courseLink in RaceHtmlScraper.ExtractCourseLinksFromHtml(html, pageUrl))
                    {
                        if (!visitedPages.Contains(StripFragment(courseLink)) && !IsBlacklisted(courseLink))
                            nextLevel.Add((courseLink, true));
                    }
                }
            }

            currentLevel = nextLevel;
        }

        // Extract image: prefer the most prominent image from a page that had GPX links,
        // then course pages, then the start page.
        Uri? imageUrl = null;
        foreach (var (pageUri, html) in pagesWithGpx)
        {
            imageUrl = RaceHtmlScraper.ExtractProminentImage(html, pageUri);
            if (imageUrl is not null) break;
        }
        if (imageUrl is null)
        {
            foreach (var cp in coursePages)
            {
                imageUrl = RaceHtmlScraper.ExtractProminentImage(cp.Html, cp.Url);
                if (imageUrl is not null) break;
            }
        }
        imageUrl ??= startPageHtml is not null ? RaceHtmlScraper.ExtractProminentImage(startPageHtml, startUrl) : null;

        // Extract logo from the start page.
        var logoUrl = startPageHtml is not null ? RaceHtmlScraper.ExtractLogo(startPageHtml, startUrl) : null;

        var startPageDate = startPageHtml is not null ? RaceHtmlScraper.ExtractDate(startPageHtml) : null;

        // Extract event name from headings/title/meta across start page and course pages.
        var courseHtmls = coursePages.Select(cp => (cp.Url, cp.Html)).ToList();
        var extractedName = RaceHtmlScraper.ExtractEventName(startUrl, startPageHtml, courseHtmls);

        if (gpxUrlToPageHtml.Count == 0)
            return new BfsResult([], imageUrl, logoUrl, startPageDate, coursePages, extractedName);

        // Extract per-GPX dates and elevation gain from the source page where each GPX link was found.
        var gpxUrlDates = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        var gpxUrlElevation = new Dictionary<string, double?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (gpxUrl, html) in gpxUrlToPageHtml)
        {
            gpxUrlDates[gpxUrl] = RaceHtmlScraper.ExtractDate(html);
            gpxUrlElevation[gpxUrl] = RaceHtmlScraper.ExtractElevationGain(html);
        }

        // Fetch all GPX URLs concurrently.
        var gpxTasks = gpxUrlToPageHtml.Keys.Select(url => TryFetchGpxFromUrlAsync(httpClient, new Uri(url), cancellationToken));
        var gpxResults = await Task.WhenAll(gpxTasks);

        var routes = gpxResults
            .Where(r => r.HasValue)
            .Select(r =>
            {
                var (route, gpxUrl) = r!.Value;
                var date = gpxUrlDates.GetValueOrDefault(gpxUrl.AbsoluteUri);
                var elevation = gpxUrlElevation.GetValueOrDefault(gpxUrl.AbsoluteUri);
                return (route, gpxUrl, Date: date, ElevationGain: elevation);
            })
            .ToList();
        return new BfsResult(routes, imageUrl, logoUrl, startPageDate, coursePages, extractedName);
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
        var secondaryUrls = RaceHtmlScraper.ExtractGpxLinksFromHtml(content, url)
            .Concat(RaceHtmlScraper.ExtractDownloadLinksFromHtml(content, url))
            .Select(u => u.AbsoluteUri)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(u => new Uri(u))
            .ToList();

        var secondaryTasks = secondaryUrls.Select(async link =>
        {
            var linkContent = await TryFetchStringAsync(httpClient, link, cancellationToken);
            if (linkContent is null) return ((ParsedGpxRoute, Uri)?)null;
            var linkParsed = GpxParser.TryParseRoute(linkContent);
            return linkParsed is not null ? (linkParsed, link) : null;
        });

        var secondaryResults = await Task.WhenAll(secondaryTasks);
        return secondaryResults.FirstOrDefault(r => r.HasValue);
    }

    private async Task<string?> TryFetchStringAsync(HttpClient httpClient, Uri url, CancellationToken cancellationToken)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));
            return await httpClient.GetStringAsync(url, cts.Token);
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
}
