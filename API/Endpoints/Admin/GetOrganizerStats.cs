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

        var aggregateStatsTask = raceOrganizerClient.ExecuteQueryAsync<AggregateStats>(
            new QueryDefinition(
                "SELECT " +
                "COUNT(1) AS total, " +
                "SUM(IIF(IS_DEFINED(c.lastAssembledUtc), 1, 0)) AS assembled, " +
                "SUM(IIF(IS_DEFINED(c.discovery), 1, 0)) AS withAnyDiscovery, " +
                "SUM(IIF(IS_DEFINED(c.scrapers), 1, 0)) AS withAnyScraper, " +
                "SUM(IIF(IS_DEFINED(c.discovery) AND IS_DEFINED(c.scrapers), 1, 0)) AS withBoth, " +
                "SUM(IIF(NOT IS_DEFINED(c.discovery) AND NOT IS_DEFINED(c.scrapers), 1, 0)) AS noDiscoveryNoScraper, " +

                "SUM(IIF(IS_DEFINED(c.discovery[\"utmb\"]), 1, 0)) AS Utmb, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"duv\"]), 1, 0)) AS Duv, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"itra\"]), 1, 0)) AS Itra, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"tracedetrail\"]), 1, 0)) AS TraceDeTrail, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"runagain\"]), 1, 0)) AS RunAgain, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"lopplistan\"]), 1, 0)) AS Lopplistan, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"loppkartan\"]), 1, 0)) AS Loppkartan, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"betrail\"]), 1, 0)) AS BeTrail, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"manual\"]), 1, 0)) AS Manual, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"manual-mistral\"]), 1, 0)) AS ManualMistral, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"trailrunningsweden\"]), 1, 0)) AS TrailrunningSweden, " +

                "SUM(IIF(IS_DEFINED(c.discovery[\"utmb\"]) AND NOT IS_DEFINED(c.discovery[\"duv\"]) AND NOT IS_DEFINED(c.discovery[\"itra\"]) AND NOT IS_DEFINED(c.discovery[\"tracedetrail\"]) AND NOT IS_DEFINED(c.discovery[\"runagain\"]) AND NOT IS_DEFINED(c.discovery[\"loppkartan\"]) AND NOT IS_DEFINED(c.discovery[\"betrail\"]) AND NOT IS_DEFINED(c.discovery[\"manual\"]) AND NOT IS_DEFINED(c.discovery[\"manual-mistral\"]), 1, 0)) AS UtmbExclusive, " +
                "SUM(IIF(NOT IS_DEFINED(c.discovery[\"utmb\"]) AND IS_DEFINED(c.discovery[\"duv\"]) AND NOT IS_DEFINED(c.discovery[\"itra\"]) AND NOT IS_DEFINED(c.discovery[\"tracedetrail\"]) AND NOT IS_DEFINED(c.discovery[\"runagain\"]) AND NOT IS_DEFINED(c.discovery[\"loppkartan\"]) AND NOT IS_DEFINED(c.discovery[\"betrail\"]) AND NOT IS_DEFINED(c.discovery[\"manual\"]) AND NOT IS_DEFINED(c.discovery[\"manual-mistral\"]), 1, 0)) AS DuvExclusive, " +
                "SUM(IIF(NOT IS_DEFINED(c.discovery[\"utmb\"]) AND NOT IS_DEFINED(c.discovery[\"duv\"]) AND IS_DEFINED(c.discovery[\"itra\"]) AND NOT IS_DEFINED(c.discovery[\"tracedetrail\"]) AND NOT IS_DEFINED(c.discovery[\"runagain\"]) AND NOT IS_DEFINED(c.discovery[\"loppkartan\"]) AND NOT IS_DEFINED(c.discovery[\"betrail\"]) AND NOT IS_DEFINED(c.discovery[\"manual\"]) AND NOT IS_DEFINED(c.discovery[\"manual-mistral\"]), 1, 0)) AS ItraExclusive, " +
                "SUM(IIF(NOT IS_DEFINED(c.discovery[\"utmb\"]) AND NOT IS_DEFINED(c.discovery[\"duv\"]) AND NOT IS_DEFINED(c.discovery[\"itra\"]) AND IS_DEFINED(c.discovery[\"tracedetrail\"]) AND NOT IS_DEFINED(c.discovery[\"runagain\"]) AND NOT IS_DEFINED(c.discovery[\"loppkartan\"]) AND NOT IS_DEFINED(c.discovery[\"betrail\"]) AND NOT IS_DEFINED(c.discovery[\"manual\"]) AND NOT IS_DEFINED(c.discovery[\"manual-mistral\"]), 1, 0)) AS TraceDeTrailExclusive, " +
                "SUM(IIF(NOT IS_DEFINED(c.discovery[\"utmb\"]) AND NOT IS_DEFINED(c.discovery[\"duv\"]) AND NOT IS_DEFINED(c.discovery[\"itra\"]) AND NOT IS_DEFINED(c.discovery[\"tracedetrail\"]) AND IS_DEFINED(c.discovery[\"runagain\"]) AND NOT IS_DEFINED(c.discovery[\"loppkartan\"]) AND NOT IS_DEFINED(c.discovery[\"betrail\"]) AND NOT IS_DEFINED(c.discovery[\"manual\"]) AND NOT IS_DEFINED(c.discovery[\"manual-mistral\"]), 1, 0)) AS RunAgainExclusive, " +
                "SUM(IIF(NOT IS_DEFINED(c.discovery[\"utmb\"]) AND NOT IS_DEFINED(c.discovery[\"duv\"]) AND NOT IS_DEFINED(c.discovery[\"itra\"]) AND NOT IS_DEFINED(c.discovery[\"tracedetrail\"]) AND NOT IS_DEFINED(c.discovery[\"runagain\"]) AND IS_DEFINED(c.discovery[\"lopplistan\"]) AND NOT IS_DEFINED(c.discovery[\"loppkartan\"]) AND NOT IS_DEFINED(c.discovery[\"betrail\"]) AND NOT IS_DEFINED(c.discovery[\"manual\"]) AND NOT IS_DEFINED(c.discovery[\"manual-mistral\"]), 1, 0)) AS LopplistanExclusive, " +
                "SUM(IIF(NOT IS_DEFINED(c.discovery[\"utmb\"]) AND NOT IS_DEFINED(c.discovery[\"duv\"]) AND NOT IS_DEFINED(c.discovery[\"itra\"]) AND NOT IS_DEFINED(c.discovery[\"tracedetrail\"]) AND NOT IS_DEFINED(c.discovery[\"runagain\"]) AND NOT IS_DEFINED(c.discovery[\"lopplistan\"]) AND IS_DEFINED(c.discovery[\"loppkartan\"]) AND NOT IS_DEFINED(c.discovery[\"betrail\"]) AND NOT IS_DEFINED(c.discovery[\"manual\"]) AND NOT IS_DEFINED(c.discovery[\"manual-mistral\"]), 1, 0)) AS LoppkartanExclusive, " +
                "SUM(IIF(NOT IS_DEFINED(c.discovery[\"utmb\"]) AND NOT IS_DEFINED(c.discovery[\"duv\"]) AND NOT IS_DEFINED(c.discovery[\"itra\"]) AND NOT IS_DEFINED(c.discovery[\"tracedetrail\"]) AND NOT IS_DEFINED(c.discovery[\"runagain\"]) AND NOT IS_DEFINED(c.discovery[\"lopplistan\"]) AND NOT IS_DEFINED(c.discovery[\"loppkartan\"]) AND IS_DEFINED(c.discovery[\"betrail\"]) AND NOT IS_DEFINED(c.discovery[\"manual\"]) AND NOT IS_DEFINED(c.discovery[\"manual-mistral\"]), 1, 0)) AS BeTrailExclusive, " +
                "SUM(IIF(NOT IS_DEFINED(c.discovery[\"utmb\"]) AND NOT IS_DEFINED(c.discovery[\"duv\"]) AND NOT IS_DEFINED(c.discovery[\"itra\"]) AND NOT IS_DEFINED(c.discovery[\"tracedetrail\"]) AND NOT IS_DEFINED(c.discovery[\"runagain\"]) AND NOT IS_DEFINED(c.discovery[\"loppkartan\"]) AND NOT IS_DEFINED(c.discovery[\"betrail\"]) AND IS_DEFINED(c.discovery[\"manual\"]) AND NOT IS_DEFINED(c.discovery[\"manual-mistral\"]), 1, 0)) AS ManualExclusive, " +
                "SUM(IIF(NOT IS_DEFINED(c.discovery[\"utmb\"]) AND NOT IS_DEFINED(c.discovery[\"duv\"]) AND NOT IS_DEFINED(c.discovery[\"itra\"]) AND NOT IS_DEFINED(c.discovery[\"tracedetrail\"]) AND NOT IS_DEFINED(c.discovery[\"runagain\"]) AND NOT IS_DEFINED(c.discovery[\"loppkartan\"]) AND NOT IS_DEFINED(c.discovery[\"betrail\"]) AND NOT IS_DEFINED(c.discovery[\"manual\"]) AND IS_DEFINED(c.discovery[\"manual-mistral\"]), 1, 0)) AS ManualMistralExclusive, " +
                "SUM(IIF(NOT IS_DEFINED(c.discovery[\"utmb\"]) AND NOT IS_DEFINED(c.discovery[\"duv\"]) AND NOT IS_DEFINED(c.discovery[\"itra\"]) AND NOT IS_DEFINED(c.discovery[\"tracedetrail\"]) AND NOT IS_DEFINED(c.discovery[\"runagain\"]) AND NOT IS_DEFINED(c.discovery[\"loppkartan\"]) AND NOT IS_DEFINED(c.discovery[\"betrail\"]) AND NOT IS_DEFINED(c.discovery[\"manual\"]) AND NOT IS_DEFINED(c.discovery[\"manual-mistral\"]) AND IS_DEFINED(c.discovery[\"trailrunningsweden\"]), 1, 0)) AS TrailrunningSwedenExclusive, " +

                "SUM(IIF(IS_DEFINED(c.discovery[\"utmb\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"utmb\"] WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)), 1, 0)) AS UtmbMissingGeometry, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"duv\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"duv\"] WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)), 1, 0)) AS DuvMissingGeometry, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"itra\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"itra\"] WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)), 1, 0)) AS ItraMissingGeometry, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"tracedetrail\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"tracedetrail\"] WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)), 1, 0)) AS TraceDeTrailMissingGeometry, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"runagain\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"runagain\"] WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)), 1, 0)) AS RunAgainMissingGeometry, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"lopplistan\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"lopplistan\"] WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)), 1, 0)) AS LopplistanMissingGeometry, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"loppkartan\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"loppkartan\"] WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)), 1, 0)) AS LoppkartanMissingGeometry, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"betrail\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"betrail\"] WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)), 1, 0)) AS BeTrailMissingGeometry, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"manual\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"manual\"] WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)), 1, 0)) AS ManualMissingGeometry, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"manual-mistral\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"manual-mistral\"] WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)), 1, 0)) AS ManualMistralMissingGeometry, " +
                "SUM(IIF(IS_DEFINED(c.discovery[\"trailrunningsweden\"]) AND EXISTS(SELECT VALUE d FROM d IN c.discovery[\"trailrunningsweden\"] WHERE NOT IS_DEFINED(d.latitude) OR NOT IS_DEFINED(d.longitude)), 1, 0)) AS TrailrunningSwedenMissingGeometry, " +

                "SUM(IIF(IS_DEFINED(c.scrapers[\"utmb\"]), 1, 0)) AS utmbTotal, " +
                "SUM(IIF(IS_DEFINED(c.scrapers[\"itra\"]), 1, 0)) AS itraTotal, " +
                "SUM(IIF(IS_DEFINED(c.scrapers[\"bfs\"]), 1, 0)) AS bfsTotal, " +
                "SUM(IIF(IS_DEFINED(c.scrapers[\"utmb\"]) AND IS_DEFINED(c.scrapers[\"utmb\"].routes) AND ARRAY_LENGTH(c.scrapers[\"utmb\"].routes) > 0, 1, 0)) AS utmbWithRoutes, " +
                "SUM(IIF(IS_DEFINED(c.scrapers[\"itra\"]) AND IS_DEFINED(c.scrapers[\"itra\"].routes) AND ARRAY_LENGTH(c.scrapers[\"itra\"].routes) > 0, 1, 0)) AS itraWithRoutes, " +
                "SUM(IIF(IS_DEFINED(c.scrapers[\"bfs\"]) AND IS_DEFINED(c.scrapers[\"bfs\"].routes) AND ARRAY_LENGTH(c.scrapers[\"bfs\"].routes) > 0, 1, 0)) AS bfsWithRoutes " +
                "FROM c"), requestOptions: queryRequestOptions);

        // ── Wait for everything ───────────────────────────────────────────────
        await Task.WhenAll(
            aggregateStatsTask);

        // ── Derived totals ────────────────────────────────────────────────────
        var aggregateStats = aggregateStatsTask.Result.FirstOrDefault() ?? new AggregateStats(
            0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        var totalExclusive =
            aggregateStats.UtmbExclusive +
            aggregateStats.DuvExclusive +
            aggregateStats.ItraExclusive +
            aggregateStats.TraceDeTrailExclusive +
            aggregateStats.RunAgainExclusive +
            aggregateStats.LopplistanExclusive +
            aggregateStats.LoppkartanExclusive +
            aggregateStats.BeTrailExclusive +
            aggregateStats.ManualExclusive +
            aggregateStats.ManualMistralExclusive +
            aggregateStats.TrailrunningSwedenExclusive;

        var body = new
        {
            overview = new
            {
                aggregateStats.Total,
                aggregateStats.Assembled,
                neverAssembled = aggregateStats.Total - aggregateStats.Assembled,
                withAnyDiscovery = aggregateStats.WithAnyDiscovery,
                withAnyScraper = aggregateStats.WithAnyScraper,
                withBothDiscoveryAndScraper = aggregateStats.WithBoth,
                noDiscoveryNoScraper = aggregateStats.NoDiscoveryNoScraper,
            },
            discoveryAgents = new Dictionary<string, object>
            {
                ["utmb"] = new { discoveries = aggregateStats.Utmb, missingGeometry = aggregateStats.UtmbMissingGeometry, exclusive = aggregateStats.UtmbExclusive },
                ["duv"] = new { discoveries = aggregateStats.Duv, missingGeometry = aggregateStats.DuvMissingGeometry, exclusive = aggregateStats.DuvExclusive },
                ["itra"] = new { discoveries = aggregateStats.Itra, missingGeometry = aggregateStats.ItraMissingGeometry, exclusive = aggregateStats.ItraExclusive },
                ["tracedetrail"] = new { discoveries = aggregateStats.TraceDeTrail, missingGeometry = aggregateStats.TraceDeTrailMissingGeometry, exclusive = aggregateStats.TraceDeTrailExclusive },
                ["runagain"] = new { discoveries = aggregateStats.RunAgain, missingGeometry = aggregateStats.RunAgainMissingGeometry, exclusive = aggregateStats.RunAgainExclusive },
                ["lopplistan"] = new { discoveries = aggregateStats.Lopplistan, missingGeometry = aggregateStats.LopplistanMissingGeometry, exclusive = aggregateStats.LopplistanExclusive },
                ["loppkartan"] = new { discoveries = aggregateStats.Loppkartan, missingGeometry = aggregateStats.LoppkartanMissingGeometry, exclusive = aggregateStats.LoppkartanExclusive },
                ["betrail"] = new { discoveries = aggregateStats.BeTrail, missingGeometry = aggregateStats.BeTrailMissingGeometry, exclusive = aggregateStats.BeTrailExclusive },
                ["manual"] = new { discoveries = aggregateStats.Manual, missingGeometry = aggregateStats.ManualMissingGeometry, exclusive = aggregateStats.ManualExclusive },
                ["manual-mistral"] = new { discoveries = aggregateStats.ManualMistral, missingGeometry = aggregateStats.ManualMistralMissingGeometry, exclusive = aggregateStats.ManualMistralExclusive },
                ["trailrunningsweden"] = new { discoveries = aggregateStats.TrailrunningSweden, missingGeometry = aggregateStats.TrailrunningSwedenMissingGeometry, exclusive = aggregateStats.TrailrunningSwedenExclusive },
            },
            discoveryOverlap = new
            {
                discoveredByExactlyOneAgent = totalExclusive,
                discoveredByMultipleAgents = aggregateStats.WithAnyDiscovery - totalExclusive,
            },
            scrapers = new Dictionary<string, object>
            {
                ["utmb"] = new { total = aggregateStats.UtmbTotal, withRoutes = aggregateStats.UtmbWithRoutes, withoutRoutes = aggregateStats.UtmbTotal - aggregateStats.UtmbWithRoutes },
                ["itra"] = new { total = aggregateStats.ItraTotal, withRoutes = aggregateStats.ItraWithRoutes, withoutRoutes = aggregateStats.ItraTotal - aggregateStats.ItraWithRoutes },
                ["bfs"] = new { total = aggregateStats.BfsTotal, withRoutes = aggregateStats.BfsWithRoutes, withoutRoutes = aggregateStats.BfsTotal - aggregateStats.BfsWithRoutes },
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

    private record AggregateStats(
        int Total,
        int Assembled,
        int WithAnyDiscovery,
        int WithAnyScraper,
        int WithBoth,
        int NoDiscoveryNoScraper,
        int Utmb,
        int Duv,
        int Itra,
        int TraceDeTrail,
        int RunAgain,
        int Lopplistan,
        int Loppkartan,
        int BeTrail,
        int Manual,
        int ManualMistral,
            int TrailrunningSweden,
            int UtmbExclusive,
            int DuvExclusive,
            int ItraExclusive,
            int TraceDeTrailExclusive,
            int RunAgainExclusive,
            int LopplistanExclusive,
            int LoppkartanExclusive,
            int BeTrailExclusive,
            int ManualExclusive,
            int ManualMistralExclusive,
            int TrailrunningSwedenExclusive,
            int UtmbMissingGeometry,
            int DuvMissingGeometry,
            int ItraMissingGeometry,
            int TraceDeTrailMissingGeometry,
            int RunAgainMissingGeometry,
            int LopplistanMissingGeometry,
            int LoppkartanMissingGeometry,
            int BeTrailMissingGeometry,
            int ManualMissingGeometry,
            int ManualMistralMissingGeometry,
            int TrailrunningSwedenMissingGeometry,
            int UtmbTotal,
            int ItraTotal,
            int BfsTotal,
            int UtmbWithRoutes,
            int ItraWithRoutes,
            int BfsWithRoutes);

}

