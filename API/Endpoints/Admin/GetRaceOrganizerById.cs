using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public record AdminRaceDiscoveryEntry(
    string Source,
    string? Name,
    string? Date,
    string? Distance,
    string? RaceType,
    string? WebsiteUrl,
    string? ImageUrl,
    string? StartFee,
    string? Currency,
    IReadOnlyList<string> SourceUrls);

public record AdminRaceScraperEntry(
    string Scraper,
    string? WebsiteUrl,
    string? ImageUrl,
    string? StartFee,
    string? Currency,
    int RouteCount);

public record AdminRaceOrganizerDetail(
    string OrganizerKey,
    string Url,
    string? LastScrapedUtc,
    string? LastAssembledUtc,
    IReadOnlyDictionary<string, IReadOnlyList<AdminRaceDiscoveryEntry>> Discovery,
    IReadOnlyDictionary<string, AdminRaceScraperEntry> Scrapers);

public class GetRaceOrganizerById(
    BlobOrganizerStore organizerStore,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"], Summary = "Inspect one race organizer working document.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "organizerKey", In = ParameterLocation.Path, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(AdminRaceOrganizerDetail))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NotFound)]
    [Function(nameof(GetRaceOrganizerById))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/races/organizers/{organizerKey}")] HttpRequestData req,
        string organizerKey,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var doc = await organizerStore.GetByIdAsync(organizerKey, cancellationToken);
        if (doc is null)
            return req.CreateResponse(HttpStatusCode.NotFound);

        var discovery = (doc.Discovery ?? [])
            .ToDictionary(
                kv => kv.Key,
                kv => (IReadOnlyList<AdminRaceDiscoveryEntry>)kv.Value.Select(entry => new AdminRaceDiscoveryEntry(
                    kv.Key,
                    entry.Name,
                    entry.Date,
                    entry.Distance,
                    entry.RaceType,
                    GetDiscoveryWebsiteUrl(entry),
                    entry.ImageUrl,
                    entry.StartFee,
                    entry.Currency,
                    entry.SourceUrls ?? [])).ToArray(),
                StringComparer.OrdinalIgnoreCase);

        var scrapers = (doc.Scrapers ?? [])
            .ToDictionary(
                kv => kv.Key,
                kv => new AdminRaceScraperEntry(
                    kv.Key,
                    kv.Value.WebsiteUrl,
                    kv.Value.ImageUrl,
                    kv.Value.StartFee,
                    kv.Value.Currency,
                    kv.Value.Routes?.Count ?? 0),
                StringComparer.OrdinalIgnoreCase);

        var detail = new AdminRaceOrganizerDetail(
            doc.Id,
            doc.Url,
            doc.LastScrapedUtc,
            doc.LastAssembledUtc,
            discovery,
            scrapers);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(detail, cancellationToken);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey)) return false;
        return req.Headers.TryGetValues("x-admin-key", out var provided) && provided.FirstOrDefault() == adminKey;
    }

    private static string? GetDiscoveryWebsiteUrl(SourceDiscovery entry)
    {
        if (entry.SourceUrls is { Count: > 0 })
        {
            var preferred = entry.SourceUrls.FirstOrDefault(url =>
                Uri.TryCreate(url, UriKind.Absolute, out var uri)
                && !uri.Host.Contains("mittlopp.se", StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrWhiteSpace(preferred))
                return preferred;

            return entry.SourceUrls[0];
        }

        return null;
    }
}
