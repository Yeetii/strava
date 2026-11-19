using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Stigar.Core.Models;
using Stigar.Core.Services;

namespace Stigar.Core.Services.Scrapers;

/// <summary>
/// Scrapes PlotARoute routes from public route URLs and organizer pages linking to them.
/// PlotARoute exposes route geometry via get_route.asp?RouteID={id} as JSON.
/// </summary>
internal sealed partial class PlotARouteScraper(ILogger logger)
{
    [GeneratedRegex(@"(?:https?://(?:www\.)?plotaroute\.com)?/route/(?<id>\d+)(?:\b|/|\?)", RegexOptions.IgnoreCase)]
    private static partial Regex PlotARouteRouteRegex();

    private int _cloudflareChallengeLogged;

    // PlotARoute route IDs are positive integers. Zero is a common placeholder in
    // organizer HTML (e.g. JS templates) and must never be fetched.
    private static bool IsValidRouteId(string routeId) => long.TryParse(routeId, out var id) && id > 0;

    private static bool IsCloudflareChallenge(HttpResponseMessage response) =>
        response.Headers.TryGetValues("cf-mitigated", out var values) &&
        values.Any(value => value.Contains("challenge", StringComparison.OrdinalIgnoreCase));

    public static IReadOnlyList<string> ExtractRouteIds(string html)
    {
        return [.. PlotARouteRouteRegex()
            .Matches(html)
            .Select(match => match.Groups["id"].Value)
            .Where(IsValidRouteId)
            .Distinct(StringComparer.OrdinalIgnoreCase)];
    }

    public static bool TryGetRouteId(Uri uri, out string routeId)
    {
        routeId = string.Empty;
        var match = PlotARouteRouteRegex().Match(uri.AbsoluteUri);
        if (!match.Success)
            return false;

        routeId = match.Groups["id"].Value;
        return IsValidRouteId(routeId);
    }

    public async Task<ScrapedRoute?> ScrapeRouteAsync(
        string routeId,
        Uri sourcePageUrl,
        HttpClient httpClient,
        CancellationToken cancellationToken)
    {
        if (!IsValidRouteId(routeId))
            return null;

        var url = new Uri($"https://www.plotaroute.com/get_route.asp?RouteID={routeId}");
        PlotARouteRouteJson? data;

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            if (httpClient.DefaultRequestHeaders.UserAgent.Count == 0)
                request.Headers.UserAgent.ParseAdd("Mozilla/5.0 (compatible; Peakshunters/1.0)");

            using var response = await httpClient.SendAsync(request, cancellationToken);

            // PlotARoute sits behind a Cloudflare managed JavaScript challenge that
            // a plain HttpClient cannot solve. The whole domain returns 403 to
            // automated clients, so treat it as an unavailable source rather than
            // logging a warning per route. Log once at debug level and degrade.
            if (IsCloudflareChallenge(response))
            {
                if (Interlocked.Exchange(ref _cloudflareChallengeLogged, 1) == 0)
                    logger.LogDebug("PlotARoute: blocked by Cloudflare challenge; skipping PlotARoute routes");
                return null;
            }

            response.EnsureSuccessStatusCode();
            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            data = await JsonSerializer.DeserializeAsync<PlotARouteRouteJson>(stream, cancellationToken: cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "PlotARoute: failed to fetch route {Id}", routeId);
            return null;
        }

        if (string.IsNullOrWhiteSpace(data?.RouteData))
        {
            logger.LogDebug("PlotARoute: route {Id} has no RouteData", routeId);
            return null;
        }

        PlotARoutePoint[]? points;
        try
        {
            points = JsonSerializer.Deserialize<PlotARoutePoint[]>(data.RouteData);
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "PlotARoute: failed to parse RouteData for route {Id}", routeId);
            return null;
        }

        if (points is null || points.Length < 2)
        {
            logger.LogDebug("PlotARoute: route {Id} has insufficient points", routeId);
            return null;
        }

        var coordinates = points
            .Where(point => point is { Latitude: not null, Longitude: not null })
            .Select(point => new Coordinate(point!.Longitude!.Value, point.Latitude!.Value))
            .ToList();

        if (coordinates.Count < 2)
            return null;

        var distanceKm = data.Distance > 0 ? data.Distance / 1000.0 : GpxParser.CalculateDistanceKm(coordinates);

        return new ScrapedRoute(
            Coordinates: coordinates,
            SourceUrl: sourcePageUrl,
            Name: string.IsNullOrWhiteSpace(data.RouteName) ? "Unnamed" : data.RouteName,
            Distance: RaceScrapeDiscovery.FormatDistanceKm(distanceKm),
            ElevationGain: data.Ascent > 0 ? data.Ascent : null,
            GpxUrl: url,
            GpxSource: GpxSourceKind.PlotARoute);
    }

    private sealed class PlotARouteRouteJson
    {
        [JsonPropertyName("RouteData")] public string? RouteData { get; init; }
        [JsonPropertyName("RouteName")] public string? RouteName { get; init; }
        [JsonPropertyName("Distance")] public double Distance { get; init; }
        [JsonPropertyName("Ascent")] public double Ascent { get; init; }
    }

    private sealed class PlotARoutePoint
    {
        [JsonPropertyName("lat")] public double? Latitude { get; init; }
        [JsonPropertyName("lng")] public double? Longitude { get; init; }
    }
}