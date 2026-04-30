using System.Net;
using System.Text;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public class GetOrganizerStats(
    BlobOrganizerStore organizerStore,
    IConfiguration configuration)
{
    private static readonly string[] DiscoverySources =
    [
        "utmb",
        "duv",
        "itra",
        "tracedetrail",
        "runagain",
        "lopplistan",
        "loppkartan",
        "betrail",
        "manual",
        "manual-mistral",
        "trailrunningsweden",
        "skyrunning",
    ];

    private static readonly string[] ScraperSources = ["utmb", "itra", "bfs"];

    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(object))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetOrganizerStats))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/organizers/stats")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        int total = 0, withAnyDiscovery = 0, withAnyScraper = 0, withBoth = 0, noDiscoveryNoScraper = 0;
        var activeOrganizerIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var bySource = DiscoverySources.ToDictionary(
            s => s,
            _ => (count: 0, missingGeometry: 0, couldBeFilledFromLocation: 0),
            StringComparer.OrdinalIgnoreCase);
        var scraperTotal = ScraperSources.ToDictionary(s => s, _ => 0, StringComparer.OrdinalIgnoreCase);
        var scraperWithRoutes = ScraperSources.ToDictionary(s => s, _ => 0, StringComparer.OrdinalIgnoreCase);

        await foreach (var doc in organizerStore.StreamAllAsync(maxConcurrency: 32))
        {
            total++;
            activeOrganizerIds.Add(doc.Id);
            var hasDiscovery = doc.Discovery is { Count: > 0 };
            var hasScraper = doc.Scrapers is { Count: > 0 };
            if (hasDiscovery) withAnyDiscovery++;
            if (hasScraper) withAnyScraper++;
            if (hasDiscovery && hasScraper) withBoth++;
            if (!hasDiscovery && !hasScraper) noDiscoveryNoScraper++;

            if (doc.Discovery is not null)
            {
                foreach (var source in DiscoverySources)
                {
                    if (!doc.Discovery.TryGetValue(source, out var discoveries) || discoveries is not { Count: > 0 })
                        continue;

                    var (count, missingGeo, couldFill) = bySource[source];
                    count++;
                    foreach (var d in discoveries)
                    {
                        if (d.Latitude is null || d.Longitude is null)
                        {
                            missingGeo++;
                            if (!string.IsNullOrEmpty(d.Location))
                                couldFill++;
                        }
                    }
                    bySource[source] = (count, missingGeo, couldFill);
                }
            }

            if (doc.Scrapers is not null)
            {
                foreach (var source in ScraperSources)
                {
                    if (!doc.Scrapers.TryGetValue(source, out var output))
                        continue;
                    scraperTotal[source]++;
                    if (output.Routes is { Count: > 0 })
                        scraperWithRoutes[source]++;
                }
            }
        }

        var redirectEntries = new List<OrganizerRedirectEntry>();
        await foreach (var redirect in organizerStore.StreamRedirectsAsync(maxConcurrency: 32))
            redirectEntries.Add(redirect);

        var redirectsWithExistingTargets = redirectEntries.Count(redirect => activeOrganizerIds.Contains(redirect.TargetOrganizerKey));
        var redirectsWithMissingTargets = redirectEntries.Count - redirectsWithExistingTargets;

        var body = new
        {
            overview = new
            {
                total,
                totalRedirects = redirectEntries.Count,
                totalIncludingRedirects = total + redirectEntries.Count,
                withAnyDiscovery,
                withAnyScraper,
                withBothDiscoveryAndScraper = withBoth,
                noDiscoveryNoScraper,
            },
            redirects = new
            {
                total = redirectEntries.Count,
                withExistingTarget = redirectsWithExistingTargets,
                withMissingTarget = redirectsWithMissingTargets,
            },
            discoveryAgents = DiscoverySources.ToDictionary(
                s => s,
                s => (object)new
                {
                    discoveries = bySource[s].count,
                    missingGeometry = bySource[s].missingGeometry,
                    couldBeFilledFromLocation = bySource[s].couldBeFilledFromLocation,
                },
                StringComparer.OrdinalIgnoreCase),
            scrapers = ScraperSources.ToDictionary(
                s => s,
                s => (object)new
                {
                    total = scraperTotal[s],
                    withRoutes = scraperWithRoutes[s],
                    withoutRoutes = scraperTotal[s] - scraperWithRoutes[s],
                },
                StringComparer.OrdinalIgnoreCase),
        };

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(body);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey)) return false;
        return req.Headers.TryGetValues("x-admin-key", out var provided) && provided.FirstOrDefault() == adminKey;
    }
}
