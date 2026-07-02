using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public record AdminRaceOrganizerListItem(
    string OrganizerKey,
    string Url,
    IReadOnlyList<string> DiscoverySources,
    IReadOnlyList<string> ScraperSources,
    string? LastScrapedUtc,
    string? LastAssembledUtc,
    int DiscoveryEntryCount,
    int ScraperRouteCount);

public class GetRaceOrganizers(
    BlobOrganizerStore organizerStore,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"], Summary = "List race organizers with discovery and scraper status.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "search", In = ParameterLocation.Query, Type = typeof(string), Required = false)]
    [OpenApiParameter(name: "source", In = ParameterLocation.Query, Type = typeof(string), Required = false)]
    [OpenApiParameter(name: "hasDiscovery", In = ParameterLocation.Query, Type = typeof(bool), Required = false)]
    [OpenApiParameter(name: "hasScraper", In = ParameterLocation.Query, Type = typeof(bool), Required = false)]
    [OpenApiParameter(name: "limit", In = ParameterLocation.Query, Type = typeof(int), Required = false)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IReadOnlyList<AdminRaceOrganizerListItem>))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetRaceOrganizers))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/races/organizers")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
        var search = query["search"]?.Trim();
        var source = query["source"]?.Trim();
        var hasDiscovery = ParseOptionalBool(query["hasDiscovery"]);
        var hasScraper = ParseOptionalBool(query["hasScraper"]);
        var limit = int.TryParse(query["limit"], out var parsedLimit) && parsedLimit > 0 ? parsedLimit : 200;

        var results = new List<AdminRaceOrganizerListItem>();
        await foreach (var metadata in organizerStore.StreamMetadataWithoutGeometriesAsync(maxConcurrency: 32, cancellationToken))
        {
            var discoverySources = metadata.Discovery?.Where(kv => kv.Value is { Count: > 0 }).Select(kv => kv.Key).OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToArray() ?? [];
            var scraperSources = metadata.Scrapers?.Where(kv => kv.Value is not null).Select(kv => kv.Key).OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToArray() ?? [];
            var discoveryEntryCount = metadata.Discovery?.Values.Sum(entries => entries?.Count ?? 0) ?? 0;
            var scraperRouteCount = metadata.Scrapers?.Values.Sum(output => output?.Routes?.Count ?? 0) ?? 0;

            if (hasDiscovery.HasValue && (discoverySources.Length > 0) != hasDiscovery.Value)
                continue;
            if (hasScraper.HasValue && (scraperSources.Length > 0) != hasScraper.Value)
                continue;
            if (!string.IsNullOrWhiteSpace(source)
                && !discoverySources.Contains(source, StringComparer.OrdinalIgnoreCase)
                && !scraperSources.Contains(source, StringComparer.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(search))
            {
                var haystack = string.Join(" ", [metadata.Id, metadata.Url, .. discoverySources, .. scraperSources]);
                if (!haystack.Contains(search, StringComparison.OrdinalIgnoreCase))
                    continue;
            }

            results.Add(new AdminRaceOrganizerListItem(
                metadata.Id,
                metadata.Url,
                discoverySources,
                scraperSources,
                metadata.LastScrapedUtc,
                metadata.LastAssembledUtc,
                discoveryEntryCount,
                scraperRouteCount));

            if (results.Count >= limit)
                break;
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(results, cancellationToken);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey)) return false;
        return req.Headers.TryGetValues("x-admin-key", out var provided) && provided.FirstOrDefault() == adminKey;
    }

    private static bool? ParseOptionalBool(string? raw)
        => bool.TryParse(raw, out var parsed) ? parsed : null;
}
