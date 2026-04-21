using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Services;

namespace API.Endpoints.Admin;

public class GetRaceStats(
    RaceCollectionClient raceCollectionClient,
    RaceOrganizerClient raceOrganizerClient,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(object))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetRaceStats))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/races/stats")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var totalQuery = new QueryDefinition(
            "SELECT VALUE COUNT(1) FROM c WHERE c.kind = @kind")
            .WithParameter("@kind", FeatureKinds.Race);

        var withCourseQuery = new QueryDefinition(
            "SELECT VALUE COUNT(1) FROM c WHERE c.kind = @kind AND c.geometry.type = 'LineString'")
            .WithParameter("@kind", FeatureKinds.Race);

        var pointOnlyQuery = new QueryDefinition(
            "SELECT VALUE COUNT(1) FROM c WHERE c.kind = @kind AND c.geometry.type = 'Point'")
            .WithParameter("@kind", FeatureKinds.Race);

        var withDateQuery = new QueryDefinition(
            "SELECT VALUE COUNT(1) FROM c WHERE c.kind = @kind AND IS_DEFINED(c.properties.date)")
            .WithParameter("@kind", FeatureKinds.Race);

        var withRaceTypeQuery = new QueryDefinition(
            "SELECT VALUE COUNT(1) FROM c WHERE c.kind = @kind AND IS_DEFINED(c.properties.raceType)")
            .WithParameter("@kind", FeatureKinds.Race);

        var raceTypeBreakdownQuery = new QueryDefinition(
            "SELECT c.properties.raceType AS raceType, COUNT(1) AS count FROM c WHERE c.kind = @kind AND IS_DEFINED(c.properties.raceType) GROUP BY c.properties.raceType")
            .WithParameter("@kind", FeatureKinds.Race);

        var sourceBreakdownQuery = new QueryDefinition(
            "SELECT s AS source, COUNT(1) AS count FROM c JOIN s IN c.properties.sources WHERE c.kind = @kind GROUP BY s")
            .WithParameter("@kind", FeatureKinds.Race);

        var organizersTotalQuery = new QueryDefinition(
            "SELECT VALUE COUNT(1) FROM c");

        var discoverySources = new[] { "utmb", "duv", "itra", "tracedetrail", "runagain", "loppkartan", "manual", "manual-mistral" };
        var scraperSources = new[] { "utmb", "itra", "bfs", "tracedetrail" };

        var discoveryCountTasks = discoverySources.ToDictionary(
            source => source,
            source => raceOrganizerClient.ExecuteQueryAsync<int>(
                new QueryDefinition($"SELECT VALUE COUNT(1) FROM c WHERE IS_DEFINED(c.discovery[\"{source}\"])")));

        var scraperCountTasks = scraperSources.ToDictionary(
            source => source,
            source => raceOrganizerClient.ExecuteQueryAsync<int>(
                new QueryDefinition($"SELECT VALUE COUNT(1) FROM c WHERE IS_DEFINED(c.scrapers[\"{source}\"])")));

        var totalTask = raceCollectionClient.ExecuteQueryAsync<int>(totalQuery);
        var withCourseTask = raceCollectionClient.ExecuteQueryAsync<int>(withCourseQuery);
        var pointOnlyTask = raceCollectionClient.ExecuteQueryAsync<int>(pointOnlyQuery);
        var withDateTask = raceCollectionClient.ExecuteQueryAsync<int>(withDateQuery);
        var withRaceTypeTask = raceCollectionClient.ExecuteQueryAsync<int>(withRaceTypeQuery);
        var raceTypeBreakdownTask = raceCollectionClient.ExecuteQueryAsync<RaceTypeCount>(raceTypeBreakdownQuery);
        var sourceBreakdownTask = raceCollectionClient.ExecuteQueryAsync<SourceCount>(sourceBreakdownQuery);
        var organizersTotalTask = raceOrganizerClient.ExecuteQueryAsync<int>(organizersTotalQuery);

        var discoveryCountTasksList = discoveryCountTasks.Values.ToList();
        var scraperCountTasksList = scraperCountTasks.Values.ToList();

        await Task.WhenAll(
            totalTask,
            withCourseTask,
            pointOnlyTask,
            withDateTask,
            withRaceTypeTask,
            raceTypeBreakdownTask,
            sourceBreakdownTask,
            organizersTotalTask);

        await Task.WhenAll(discoveryCountTasksList.Concat(scraperCountTasksList));

        var body = new
        {
            total = totalTask.Result.FirstOrDefault(),
            withCourse = withCourseTask.Result.FirstOrDefault(),
            pointOnly = pointOnlyTask.Result.FirstOrDefault(),
            withDate = withDateTask.Result.FirstOrDefault(),
            withRaceType = withRaceTypeTask.Result.FirstOrDefault(),
            organizersTotal = organizersTotalTask.Result.FirstOrDefault(),
            byRaceType = raceTypeBreakdownTask.Result
                .OrderByDescending(x => x.Count)
                .ToDictionary(x => x.RaceType ?? "(null)", x => x.Count),
            bySource = sourceBreakdownTask.Result
                .Where(x => x.Count > 20)
                .OrderByDescending(x => x.Count)
                .ToDictionary(x => x.Source ?? "(unknown)", x => x.Count),
            organizerDiscoveries = discoverySources.ToDictionary(
                source => source,
                source => discoveryCountTasks[source].Result.FirstOrDefault()),
            organizerScrapes = scraperSources.ToDictionary(
                source => source,
                source => scraperCountTasks[source].Result.FirstOrDefault()),
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

    private record RaceTypeCount(string? RaceType, int Count);
    private record SourceCount(string? Source, int Count);
}
