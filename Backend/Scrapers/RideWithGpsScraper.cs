using System.Net.Http.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;

namespace Backend.Scrapers;

/// <summary>
/// Scrapes race routes from RideWithGPS embedded iframes.
/// Public routes are accessible without authentication at ridewithgps.com/routes/{id}.json,
/// which returns full <c>track_points</c> with lng/lat/elevation.
///
/// Embed formats encountered in the wild:
///   &lt;iframe src="https://ridewithgps.com/embeds?type=route&amp;id={ID}&amp;..."&gt;
///   &lt;iframe src="https://ridewithgps.com/routes/{ID}/embed"&gt;
/// </summary>
internal sealed partial class RideWithGpsScraper(ILogger logger)
{
    // Matches either embed format and captures the route ID.
    // Uses a lookahead so the id= and type=route params can appear in either order.
    [GeneratedRegex(
        @"<iframe\b[^>]*\bsrc=[""']https://ridewithgps\.com/(?:embeds\?(?=[^""']*\btype=route\b)[^""']*\bid=(\d+)|routes/(\d+)/embed)[^""']*[""'][^>]*>",
        RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex RideWithGpsIframeRegex();

    /// <summary>
    /// Extracts RideWithGPS route IDs embedded as iframes in the given HTML.
    /// </summary>
    public static IReadOnlyList<string> ExtractRouteIds(string html)
    {
        return [.. RideWithGpsIframeRegex()
            .Matches(html)
            .Select(m => m.Groups[1].Success ? m.Groups[1].Value : m.Groups[2].Value)
            .Distinct(StringComparer.OrdinalIgnoreCase)];
    }

    /// <summary>
    /// Fetches track points for <paramref name="routeId"/> from the public RideWithGPS JSON endpoint
    /// and returns a <see cref="ScrapedRoute"/>, or null when the route is private or fetch fails.
    /// </summary>
    public async Task<ScrapedRoute?> ScrapeRouteAsync(
        string routeId,
        Uri sourcePageUrl,
        HttpClient httpClient,
        CancellationToken cancellationToken)
    {
        var url = new Uri($"https://ridewithgps.com/routes/{routeId}.json");
        RwgpsRouteJson? data;
        try
        {
            data = await httpClient.GetFromJsonAsync<RwgpsRouteJson>(url, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "RideWithGPS: failed to fetch route {Id}", routeId);
            return null;
        }

        if (data?.TrackPoints is null || data.TrackPoints.Length == 0)
        {
            logger.LogDebug("RideWithGPS: route {Id} has no track points (private or empty)", routeId);
            return null;
        }

        var coordinates = data.TrackPoints
            .Select(tp => new Coordinate(tp.X, tp.Y)).ToList();

        var distanceKm = GpxParser.CalculateDistanceKm(coordinates);
        var distance = RaceScrapeDiscovery.FormatDistanceKm(distanceKm);

        return new ScrapedRoute(
            Coordinates: coordinates,
            SourceUrl: sourcePageUrl,
            Name: string.IsNullOrWhiteSpace(data.Name) ? "Unnamed" : data.Name,
            Distance: distance,
            ElevationGain: data.ElevationGain > 0 ? data.ElevationGain : null,
            GpxUrl: url,
            GpxSource: GpxSourceKind.RideWithGps);
    }

    private sealed class RwgpsRouteJson
    {
        [JsonPropertyName("name")] public string? Name { get; init; }
        [JsonPropertyName("elevation_gain")] public double? ElevationGain { get; init; }
        [JsonPropertyName("track_points")] public TrackPoint[]? TrackPoints { get; init; }
    }

    private sealed class TrackPoint
    {
        [JsonPropertyName("x")] public double X { get; init; } // longitude
        [JsonPropertyName("y")] public double Y { get; init; } // latitude
    }
}
