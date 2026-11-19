using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Api.Http;

namespace Stigar.Api.Endpoints.Admin;

/// <summary>
/// Accepts one or more GPX files (as raw XML strings) alongside a race URL.
/// Parses the GPX content synchronously and writes the routes as scraper output under
/// the key "manual-gpx" — no BFS crawl needed.
/// </summary>
public class PostManualGpx(
    IOrganizerStore organizerClient,
    IConfiguration configuration,
    ILogger<PostManualGpx> logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/races/gpx",
            (HttpContext context, [FromServices] PostManualGpx endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        PostManualGpxRequest? body;
        try
        {
            body = await context.Request.ReadFromJsonAsync<PostManualGpxRequest>(cancellationToken);
        }
        catch
        {
            return BadRequest("Invalid JSON body");
        }

        if (body is null || string.IsNullOrWhiteSpace(body.Url))
            return BadRequest("Missing required field: url");

        if (body.GpxFiles is null || body.GpxFiles.Count == 0)
            return BadRequest("Missing required field: gpxFiles (must contain at least one entry)");

        if (!Uri.TryCreate(body.Url.Trim(), UriKind.Absolute, out var parsedUrl) || parsedUrl.Scheme is not ("http" or "https"))
            return BadRequest("Invalid URL — must be an absolute http(s) URL");

        // Parse each supplied GPX file.
        var routes = new List<ScrapedRouteOutput>();
        var failedCount = 0;

        foreach (var file in body.GpxFiles)
        {
            if (string.IsNullOrWhiteSpace(file.Content))
            {
                failedCount++;
                continue;
            }

            // Accept both raw XML and base64-encoded GPX (macOS base64 wraps at 76 chars).
            string xml;
            try
            {
                var stripped = file.Content.Trim().Replace("\r", "").Replace("\n", "").Replace(" ", "");
                xml = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(stripped));
            }
            catch
            {
                xml = file.Content;
            }

            var parsed = GpxParser.TryParseRoute(xml, file.Name ?? "Unnamed");
            if (parsed is null)
            {
                logger.LogWarning("PostManualGpx: failed to parse GPX for URL {Url}", parsedUrl);
                failedCount++;
                continue;
            }

            var distanceKm = GpxParser.CalculateDistanceKm(parsed.Coordinates);
            routes.Add(new ScrapedRouteOutput
            {
                Coordinates = parsed.Coordinates.Count >= 2
                    ? parsed.Coordinates.Select(c => new[] { c.Lng, c.Lat }).ToList()
                    : null,
                SourceUrl = parsedUrl.AbsoluteUri,
                Name = parsed.Name,
                Distance = file.Distance ?? Fmt.FormatDistanceKm(distanceKm),
                GpxSource = GpxSourceKind.ManualGpx,
            });
        }

        if (routes.Count == 0)
            return BadRequest($"No GPX files could be parsed ({failedCount} failed)");

        var organizerKey = OrganizerUrlRules.DeriveOrganizerKey(parsedUrl);

        // Ensure the organizer document exists with a discovery entry.
        var discovery = new SourceDiscovery
        {
            DiscoveredAtUtc = DateTime.UtcNow.ToString("o"),
            Name = body.Name,
            SourceUrls = [parsedUrl.AbsoluteUri],
        };
        await organizerClient.WriteDiscoveryAsync(organizerKey, parsedUrl.AbsoluteUri, "manual", [discovery], cancellationToken);

        // Write the parsed routes as scraper output — skips the BFS crawl entirely.
        var output = new ScraperOutput
        {
            ScrapedAtUtc = DateTime.UtcNow.ToString("o"),
            Routes = routes,
        };
        await organizerClient.WriteScraperOutputAsync(organizerKey, "manual-gpx", output, cancellationToken);
        logger.LogInformation("PostManualGpx: wrote {Count} routes for {Key}", routes.Count, Stigar.Core.Logging.LogInput.Sanitize(organizerKey));

        return TypedResults.Ok(new PostManualGpxResponse(organizerKey, parsedUrl.AbsoluteUri, routes.Count, failedCount));
    }

    private static IResult BadRequest(string message) => TypedResults.BadRequest(message);
}

public record PostManualGpxRequest(
    string Url,
    string? Name = null,
    List<ManualGpxFile>? GpxFiles = null);

public record ManualGpxFile(
    string Content,
    string? Name = null,
    string? Distance = null);

public record PostManualGpxResponse(
    string EventKey,
    string CanonicalUrl,
    int RoutesWritten,
    int ParseFailed);

file static class GpxSourceKind
{
    public const string ManualGpx = "manual_gpx";
}

file static class Fmt
{
    internal static string FormatDistanceKm(double distanceKm)
        => distanceKm == Math.Floor(distanceKm)
            ? $"{(long)distanceKm} km"
            : FormattableString.Invariant($"{distanceKm:0.#} km");
}
