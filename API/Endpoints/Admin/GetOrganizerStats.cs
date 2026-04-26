using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public class GetOrganizerStats(
    RaceOrganizerClient raceOrganizerClient,
    IConfiguration configuration)
{
    private static readonly string[] DiscoveryAgents =
    [
        "utmb", "duv", "itra", "tracedetrail", "runagain", "loppkartan", "betrail", "manual", "manual-mistral"
    ];

    private static readonly string[] ScraperAgents = ["utmb", "itra", "bfs"];

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

        // ── Overview ─────────────────────────────────────────────────────────
        var totalTask = raceOrganizerClient.ExecuteQueryAsync<int>(
            new QueryDefinition("SELECT VALUE COUNT(1) FROM c"));

        var assembledTask = raceOrganizerClient.ExecuteQueryAsync<int>(
            new QueryDefinition("SELECT VALUE COUNT(1) FROM c WHERE IS_DEFINED(c.lastAssembledUtc)"));

        var withAnyDiscoveryTask = raceOrganizerClient.ExecuteQueryAsync<int>(
            new QueryDefinition("SELECT VALUE COUNT(1) FROM c WHERE IS_DEFINED(c.discovery)"));

        var withAnyScraperTask = raceOrganizerClient.ExecuteQueryAsync<int>(
            new QueryDefinition("SELECT VALUE COUNT(1) FROM c WHERE IS_DEFINED(c.scrapers)"));

        var withBothTask = raceOrganizerClient.ExecuteQueryAsync<int>(
            new QueryDefinition(
                "SELECT VALUE COUNT(1) FROM c WHERE IS_DEFINED(c.discovery) AND IS_DEFINED(c.scrapers)"));

        var noDiscoveryNoScraperTask = raceOrganizerClient.ExecuteQueryAsync<int>(
            new QueryDefinition(
                "SELECT VALUE COUNT(1) FROM c WHERE NOT IS_DEFINED(c.discovery) AND NOT IS_DEFINED(c.scrapers)"));

        // ── Per discovery agent: organiser count ─────────────────────────────
        var agentCountTasks = DiscoveryAgents.ToDictionary(
            agent => agent,
            agent => raceOrganizerClient.ExecuteQueryAsync<int>(
                new QueryDefinition(
                    $"SELECT VALUE COUNT(1) FROM c WHERE IS_DEFINED(c.discovery[\"{agent}\"])")));

        // ── Per discovery agent: missing-geometry discovery count ────────────
        var agentMissingGeometryTasks = DiscoveryAgents.ToDictionary(
            agent => agent,
            agent => raceOrganizerClient.ExecuteQueryAsync<int>(
                new QueryDefinition(
                    $"SELECT VALUE COUNT(1) FROM c JOIN d IN c.discovery[\"{agent}\"] " +
                    $"WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)")));

        // ── Per discovery agent: exclusive count (only this agent found it) ──
        var agentExclusiveTasks = DiscoveryAgents.ToDictionary(
            agent => agent,
            agent =>
            {
                var notOthers = string.Join(" AND ",
                    DiscoveryAgents
                        .Where(a => a != agent)
                        .Select(a => $"NOT IS_DEFINED(c.discovery[\"{a}\"])"));

                return raceOrganizerClient.ExecuteQueryAsync<int>(
                    new QueryDefinition(
                        $"SELECT VALUE COUNT(1) FROM c WHERE IS_DEFINED(c.discovery[\"{agent}\"]) AND {notOthers}"));
            });

        // ── Per scraper: organiser count + how many yielded at least one route
        var scraperCountTasks = ScraperAgents.ToDictionary(
            scraper => scraper,
            scraper => raceOrganizerClient.ExecuteQueryAsync<int>(
                new QueryDefinition(
                    $"SELECT VALUE COUNT(1) FROM c WHERE IS_DEFINED(c.scrapers[\"{scraper}\"])")));

        var scraperWithRoutesTasks = ScraperAgents.ToDictionary(
            scraper => scraper,
            scraper => raceOrganizerClient.ExecuteQueryAsync<int>(
                new QueryDefinition(
                    $"SELECT VALUE COUNT(1) FROM c " +
                    $"WHERE IS_DEFINED(c.scrapers[\"{scraper}\"]) " +
                    $"AND IS_DEFINED(c.scrapers[\"{scraper}\"].routes) " +
                    $"AND ARRAY_LENGTH(c.scrapers[\"{scraper}\"].routes) > 0")));

        // ── Top organisers by number of discoveries ──────────────────────────
        var topDiscoveryOrganizersTask = raceOrganizerClient.ExecuteQueryAsync<TopDiscoveryOrganizer>(
            new QueryDefinition("SELECT c.id, c.url, c.discovery FROM c WHERE IS_DEFINED(c.discovery)"));

        // ── Wait for everything ───────────────────────────────────────────────
        var allTasks = new List<Task>
        {
            totalTask, assembledTask, withAnyDiscoveryTask, withAnyScraperTask,
            withBothTask, noDiscoveryNoScraperTask, topDiscoveryOrganizersTask,
        };
        allTasks.AddRange(agentCountTasks.Values);
        allTasks.AddRange(agentMissingGeometryTasks.Values);
        allTasks.AddRange(agentExclusiveTasks.Values);
        allTasks.AddRange(scraperCountTasks.Values);
        allTasks.AddRange(scraperWithRoutesTasks.Values);
        await Task.WhenAll(allTasks);

        // ── Derived totals ────────────────────────────────────────────────────
        var total = totalTask.Result.FirstOrDefault();
        var assembled = assembledTask.Result.FirstOrDefault();
        var withAnyDiscovery = withAnyDiscoveryTask.Result.FirstOrDefault();
        var totalExclusive = DiscoveryAgents.Sum(a => agentExclusiveTasks[a].Result.FirstOrDefault());

        var body = new
        {
            overview = new
            {
                total,
                assembled,
                neverAssembled = total - assembled,
                withAnyDiscovery,
                withAnyScraper = withAnyScraperTask.Result.FirstOrDefault(),
                withBothDiscoveryAndScraper = withBothTask.Result.FirstOrDefault(),
                noDiscoveryNoScraper = noDiscoveryNoScraperTask.Result.FirstOrDefault(),
            },
            discoveryAgents = DiscoveryAgents.ToDictionary(
                agent => agent,
                agent => (object)new
                {
                    // Organisers this agent discovered (any overlap with other agents)
                    discoveries = agentCountTasks[agent].Result.FirstOrDefault(),
                    // Discoveries for this agent missing lat/lon geometry.
                    missingGeometry = agentMissingGeometryTasks[agent].Result.FirstOrDefault(),
                    // Organisers ONLY this agent found — no other agent has them
                    exclusive = agentExclusiveTasks[agent].Result.FirstOrDefault(),
                }),
            discoveryOverlap = new
            {
                // Organisers that only a single agent ever found
                discoveredByExactlyOneAgent = totalExclusive,
                // Organisers that two or more agents independently found
                discoveredByMultipleAgents = withAnyDiscovery - totalExclusive,
            },
            scrapers = ScraperAgents.ToDictionary(
                scraper => scraper,
                scraper =>
                {
                    var scraperTotal = scraperCountTasks[scraper].Result.FirstOrDefault();
                    var withRoutes = scraperWithRoutesTasks[scraper].Result.FirstOrDefault();
                    return (object)new
                    {
                        total = scraperTotal,
                        withRoutes,
                        withoutRoutes = scraperTotal - withRoutes,
                    };
                }),
            topByDiscoveries = topDiscoveryOrganizersTask.Result
                .Select(o => new
                {
                    DiscoveryCount = o.Discovery?.Values.Sum(list => list?.Count ?? 0) ?? 0,
                    o.Id,
                    o.Url,
                    SourceUrlsByAgent = (o.Discovery ?? new Dictionary<string, List<DiscoveryUrlsOnly>>())
                        .ToDictionary(
                            kv => kv.Key,
                            kv => (kv.Value ?? new List<DiscoveryUrlsOnly>())
                                .SelectMany(d => d.SourceUrls ?? new List<string>())
                                .Where(u => !string.IsNullOrWhiteSpace(u))
                                .Distinct(StringComparer.OrdinalIgnoreCase)
                                .OrderBy(u => u, StringComparer.OrdinalIgnoreCase)
                                .ToList()),
                })
                .OrderByDescending(o => o.DiscoveryCount)
                .Take(15)
                .ToList(),
        };

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(body);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
            return false;

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
    }

    private record TopDiscoveryOrganizer(string Id, string Url, Dictionary<string, List<DiscoveryUrlsOnly>>? Discovery);
    private record DiscoveryUrlsOnly(List<string>? SourceUrls);
}
