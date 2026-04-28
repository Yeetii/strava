using System.Net;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

/// <summary>
/// Accepts one or more GPX files (as raw XML strings) alongside a race URL.
/// Parses the GPX content synchronously, writes the routes as scraper output under
/// the key "manual-gpx", and enqueues an assemble job — no BFS crawl needed.
/// </summary>
public class PostManualGpx(
    RaceOrganizerClient organizerClient,
    ServiceBusClient serviceBusClient,
    IConfiguration configuration,
    ILogger<PostManualGpx> logger)
{
    [OpenApiOperation(tags: ["Admin"], Summary = "Submit one or more GPX files for a race URL.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiRequestBody(contentType: "application/json", bodyType: typeof(PostManualGpxRequest))]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(PostManualGpxResponse))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest)]
    [Function(nameof(PostManualGpx))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/races/gpx")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        PostManualGpxRequest? body;
        try
        {
            body = await req.ReadFromJsonAsync<PostManualGpxRequest>(cancellationToken);
        }
        catch
        {
            return await BadRequest(req, "Invalid JSON body", cancellationToken);
        }

        if (body is null || string.IsNullOrWhiteSpace(body.Url))
            return await BadRequest(req, "Missing required field: url", cancellationToken);

        if (body.GpxFiles is null || body.GpxFiles.Count == 0)
            return await BadRequest(req, "Missing required field: gpxFiles (must contain at least one entry)", cancellationToken);

        if (!Uri.TryCreate(body.Url.Trim(), UriKind.Absolute, out var parsedUrl) || parsedUrl.Scheme is not ("http" or "https"))
            return await BadRequest(req, "Invalid URL — must be an absolute http(s) URL", cancellationToken);

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
            return await BadRequest(req, $"No GPX files could be parsed ({failedCount} failed)", cancellationToken);

        var organizerKey = RaceOrganizerClient.DeriveOrganizerKey(parsedUrl);

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
        logger.LogInformation("PostManualGpx: wrote {Count} routes for {Key}", routes.Count, organizerKey);

        // Enqueue assembly.
        var assembleSender = serviceBusClient.CreateSender(ServiceBusConfig.AssembleRace);
        await assembleSender.SendMessageAsync(
            new ServiceBusMessage(organizerKey) { ContentType = "text/plain" }, cancellationToken);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(
            new PostManualGpxResponse(organizerKey, parsedUrl.AbsoluteUri, routes.Count, failedCount),
            cancellationToken);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey)) return false;
        return req.Headers.TryGetValues("x-admin-key", out var provided) && provided.FirstOrDefault() == adminKey;
    }

    private static async Task<HttpResponseData> BadRequest(HttpRequestData req, string message, CancellationToken ct)
    {
        var r = req.CreateResponse(HttpStatusCode.BadRequest);
        await r.WriteStringAsync(message, ct);
        return r;
    }
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
