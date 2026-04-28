using System.Net.Http.Json;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend.Scrapers;

/// <summary>
/// Scrapes race routes from a <see href="https://racedaymap.com"/> embed.
/// Data is stored in Firebase Firestore (project maps-a8871) and is publicly readable
/// for published maps. No authentication is required.
///
/// Embed format encountered in the wild:
///   &lt;iframe src="https://app.racedaymap.com/{slug}"&gt;&lt;/iframe&gt;
///
/// Firestore query path:
///   POST https://firestore.googleapis.com/v1/projects/maps-a8871/databases/(default)/documents/maps/{slug}:runQuery
/// </summary>
internal sealed partial class RaceDayMapScraper(ILogger logger)
{
    private const string FirestoreBase =
        "https://firestore.googleapis.com/v1/projects/maps-a8871/databases/(default)/documents";

    // Matches the slug inside an app.racedaymap.com iframe src attribute.
    [GeneratedRegex(
        @"<iframe\b[^>]*\bsrc=[""']https://app\.racedaymap\.com/([A-Za-z0-9_-]+)[""'][^>]*>",
        RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex RaceDayMapIframeRegex();

    /// <summary>
    /// Extracts racedaymap slugs embedded in the given HTML page source.
    /// </summary>
    public static IReadOnlyList<string> ExtractSlugs(string html)
    {
        return RaceDayMapIframeRegex()
            .Matches(html)
            .Select(m => m.Groups[1].Value)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    /// <summary>
    /// Fetches all published routes for the given <paramref name="slug"/> from Firestore
    /// and returns one <see cref="ScrapedRoute"/> per route.  Returns null if no published
    /// version is found or if the Firestore call fails.
    /// </summary>
    public async Task<IReadOnlyList<ScrapedRoute>?> ScrapeSlugAsync(
        string slug,
        Uri sourcePageUrl,
        HttpClient httpClient,
        CancellationToken cancellationToken)
    {
        var queryUrl = $"{FirestoreBase}/maps/{slug}:runQuery";

        // Query for the latest published version of the map.
        var requestBody = new
        {
            structuredQuery = new
            {
                from = new[] { new { collectionId = "versions" } },
                where = new
                {
                    fieldFilter = new
                    {
                        field = new { fieldPath = "published" },
                        op = "EQUAL",
                        value = new { booleanValue = true }
                    }
                },
                orderBy = new[]
                {
                    new { field = new { fieldPath = "changed" }, direction = "DESCENDING" },
                    new { field = new { fieldPath = "__name__" }, direction = "DESCENDING" }
                },
                limit = 1
            }
        };

        JsonElement[] response;
        try
        {
            var httpResponse = await httpClient.PostAsJsonAsync(queryUrl, requestBody, cancellationToken);
            if (!httpResponse.IsSuccessStatusCode)
            {
                logger.LogWarning("RaceDayMap: Firestore query failed with {Status} for slug '{Slug}'",
                    httpResponse.StatusCode, slug);
                return null;
            }

            var json = await httpResponse.Content.ReadAsStringAsync(cancellationToken);
            response = JsonSerializer.Deserialize<JsonElement[]>(json) ?? [];
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "RaceDayMap: Firestore request failed for slug '{Slug}'", slug);
            return null;
        }

        if (response.Length == 0 || !response[0].TryGetProperty("document", out var doc))
        {
            logger.LogDebug("RaceDayMap: no published version found for slug '{Slug}'", slug);
            return null;
        }

        if (!doc.TryGetProperty("fields", out var fields) ||
            !fields.TryGetProperty("routes", out var routesField) ||
            !routesField.TryGetProperty("stringValue", out var routesStringEl))
        {
            logger.LogDebug("RaceDayMap: routes field missing in Firestore document for slug '{Slug}'", slug);
            return null;
        }

        JsonElement[] features;
        try
        {
            features = JsonSerializer.Deserialize<JsonElement[]>(routesStringEl.GetString() ?? "[]") ?? [];
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "RaceDayMap: failed to parse routes JSON for slug '{Slug}'", slug);
            return null;
        }

        // Download each GPX file concurrently. The URL pattern is:
        //   https://app.racedaymap.com/{slug}/{featureId}/gpx/{label}.gpx
        // This is publicly accessible for published maps and includes elevation data.
        var downloadTasks = features.Select(feature => DownloadRouteAsync(feature, slug, httpClient, cancellationToken));
        var downloaded = await Task.WhenAll(downloadTasks);

        var routes = new List<ScrapedRoute>(features.Length);
        foreach (var entry in downloaded)
        {
            if (entry is null) continue;
            var (parsedRoute, gpxUrl, label) = entry.Value;

            var distanceKm = GpxParser.CalculateDistanceKm(parsedRoute.Coordinates);
            var distance = RaceScrapeDiscovery.MatchDistanceKmToVerbose(distanceKm, label, 0.15)
                ?? RaceScrapeDiscovery.FormatDistanceKm(distanceKm);

            routes.Add(new ScrapedRoute(
                Coordinates: parsedRoute.Coordinates,
                SourceUrl: sourcePageUrl,
                Name: label ?? "Unnamed",
                Distance: distance,
                GpxUrl: gpxUrl,
                GpxSource: GpxSourceKind.RaceDayMap));
        }

        logger.LogInformation("RaceDayMap: slug '{Slug}' — {Count} routes found", slug, routes.Count);
        return routes;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Downloads and parses the GPX for a single GeoJSON feature.
    /// Returns null when the feature has no valid ID or the download/parse fails.
    /// </summary>
    private async Task<(ParsedGpxRoute Route, Uri GpxUrl, string? Label)?> DownloadRouteAsync(
        JsonElement feature,
        string slug,
        HttpClient httpClient,
        CancellationToken cancellationToken)
    {
        var featureId = feature.TryGetProperty("id", out var idEl) ? idEl.GetString() : null;
        if (string.IsNullOrEmpty(featureId)) return null;

        var props = feature.TryGetProperty("properties", out var p) ? p : default;
        var label = props.ValueKind == JsonValueKind.Object && props.TryGetProperty("label", out var l)
            ? l.GetString() : null;

        var fileName = Uri.EscapeDataString($"{label ?? "route"}.gpx");
        var gpxUrl = new Uri($"https://app.racedaymap.com/{slug}/{featureId}/gpx/{fileName}");

        string gpxContent;
        try
        {
            gpxContent = await httpClient.GetStringAsync(gpxUrl, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "RaceDayMap: failed to download GPX {Url}", gpxUrl);
            return null;
        }

        var parsed = GpxParser.TryParseRoute(gpxContent);
        if (parsed is null)
        {
            logger.LogDebug("RaceDayMap: GPX parse failed for {Url}", gpxUrl);
            return null;
        }

        return (parsed, gpxUrl, label);
    }
}
