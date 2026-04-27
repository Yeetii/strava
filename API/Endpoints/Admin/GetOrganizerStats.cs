using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
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
    private static readonly string[] DiscoverySources = new[]
    {
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
    };

    private static readonly string[] ScraperSources = new[]
    {
        "utmb",
        "itra",
        "bfs",
    };

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

        var queryRequestOptions = new QueryRequestOptions
        {
            MaxConcurrency = 1,
            MaxBufferedItemCount = 1,
        };

        var aggregateStatsTask = raceOrganizerClient.ExecuteQueryAsync<Dictionary<string, int>>(
            new QueryDefinition(BuildOrganizerStatsQuery()),
            requestOptions: queryRequestOptions);

        await Task.WhenAll(aggregateStatsTask);

        var aggregateStats = aggregateStatsTask.Result.FirstOrDefault() ?? new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        int GetStat(string name) => aggregateStats.TryGetValue(name, out var value) ? value : 0;

        var discoveryAgents = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        var totalExclusive = 0;

        foreach (var source in DiscoverySources)
        {
            var prefix = ToPascalCase(source);
            var exclusive = GetStat(prefix + "Exclusive");

            discoveryAgents[source] = new
            {
                discoveries = GetStat(prefix),
                missingGeometry = GetStat(prefix + "MissingGeometry"),
                couldBeFilledFromLocation = GetStat(prefix + "CouldBeFilledFromLocation"),
                exclusive,
            };

            totalExclusive += exclusive;
        }

        var body = new
        {
            overview = new
            {
                total = GetStat("total"),
                assembled = GetStat("assembled"),
                neverAssembled = GetStat("total") - GetStat("assembled"),
                withAnyDiscovery = GetStat("withAnyDiscovery"),
                withAnyScraper = GetStat("withAnyScraper"),
                withBothDiscoveryAndScraper = GetStat("withBoth"),
                noDiscoveryNoScraper = GetStat("noDiscoveryNoScraper"),
            },
            discoveryAgents,
            discoveryOverlap = new
            {
                discoveredByExactlyOneAgent = totalExclusive,
                discoveredByMultipleAgents = GetStat("withAnyDiscovery") - totalExclusive,
            },
            scrapers = new Dictionary<string, object>
            {
                ["utmb"] = new { total = GetStat("utmbTotal"), withRoutes = GetStat("utmbWithRoutes"), withoutRoutes = GetStat("utmbTotal") - GetStat("utmbWithRoutes") },
                ["itra"] = new { total = GetStat("itraTotal"), withRoutes = GetStat("itraWithRoutes"), withoutRoutes = GetStat("itraTotal") - GetStat("itraWithRoutes") },
                ["bfs"] = new { total = GetStat("bfsTotal"), withRoutes = GetStat("bfsWithRoutes"), withoutRoutes = GetStat("bfsTotal") - GetStat("bfsWithRoutes") },
            },
            topByDiscoveries = Array.Empty<object>(),
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

    private static string BuildOrganizerStatsQuery()
    {
        var lines = new List<string>
        {
            "SELECT",
            "  COUNT(1) AS total,",
            "  SUM(IIF(IS_DEFINED(c.lastAssembledUtc), 1, 0)) AS assembled,",
            "  SUM(IIF(IS_DEFINED(c.discovery), 1, 0)) AS withAnyDiscovery,",
            "  SUM(IIF(IS_DEFINED(c.scrapers), 1, 0)) AS withAnyScraper,",
            "  SUM(IIF(IS_DEFINED(c.discovery) AND IS_DEFINED(c.scrapers), 1, 0)) AS withBoth,",
            "  SUM(IIF(NOT IS_DEFINED(c.discovery) AND NOT IS_DEFINED(c.scrapers), 1, 0)) AS noDiscoveryNoScraper,",
        };

        foreach (var source in DiscoverySources)
        {
            lines.Add($"  SUM(IIF(IS_DEFINED(c.discovery[\"{source}\"]), 1, 0)) AS {ToPascalCase(source)},");
        }

        foreach (var source in DiscoverySources)
        {
            var exclusions = string.Join(" AND ", DiscoverySources.Where(other => other != source).Select(other => $"NOT IS_DEFINED(c.discovery[\"{other}\"])"));
            lines.Add($"  SUM(IIF(IS_DEFINED(c.discovery[\"{source}\"]) AND {exclusions}, 1, 0)) AS {ToPascalCase(source)}Exclusive,");
        }

        foreach (var source in DiscoverySources)
        {
            var prefix = ToPascalCase(source);
            lines.Add($"  SUM(IIF(IS_DEFINED(c.discovery[\"{source}\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"{source}\"] WHERE {MissingGeometryPredicate}), 1, 0)) AS {prefix}MissingGeometry,");
            lines.Add($"  SUM(IIF(IS_DEFINED(c.discovery[\"{source}\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"{source}\"] WHERE {CouldBeFilledFromLocationPredicate}), 1, 0)) AS {prefix}CouldBeFilledFromLocation,");
        }

        foreach (var source in ScraperSources)
        {
            lines.Add($"  SUM(IIF(IS_DEFINED(c.scrapers[\"{source}\"]), 1, 0)) AS {source}Total,");
        }

        foreach (var source in ScraperSources)
        {
            lines.Add($"  SUM(IIF(IS_DEFINED(c.scrapers[\"{source}\"]) AND IS_DEFINED(c.scrapers[\"{source}\"].routes) AND ARRAY_LENGTH(c.scrapers[\"{source}\"].routes) > 0, 1, 0)) AS {source}WithRoutes,");
        }

        lines[^1] = lines[^1].TrimEnd(',');
        lines.Add("FROM c");
        return string.Join("\n", lines);
    }

    private static string ToPascalCase(string source)
    {
        var builder = new StringBuilder(source.Length);
        foreach (var segment in source.Split('-', StringSplitOptions.RemoveEmptyEntries))
        {
            if (segment.Length == 0)
                continue;

            builder.Append(char.ToUpperInvariant(segment[0]));
            if (segment.Length > 1)
                builder.Append(segment.AsSpan(1));
        }

        return builder.ToString();
    }

    private static string MissingGeometryPredicate => "(NOT IS_DEFINED(d.latitude) OR IS_NULL(d.latitude) OR NOT IS_DEFINED(d.longitude) OR IS_NULL(d.longitude))";
    private static string CouldBeFilledFromLocationPredicate => MissingGeometryPredicate + " AND IS_DEFINED(d.location) AND NOT IS_NULL(d.location) AND d.location <> ''";
}
