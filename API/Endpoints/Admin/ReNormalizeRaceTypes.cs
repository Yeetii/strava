using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public class ReNormalizeRaceTypes(
    RaceCollectionClient raceCollectionClient,
    IConfiguration configuration,
    ILogger<ReNormalizeRaceTypes> logger)
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(object),
        Description = "Number of races updated.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(ReNormalizeRaceTypes))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/races/reNormalizeTypes")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        // Build a query that only fetches races whose raceType or typeLocal contains an alias token.
        var aliasKeys = RaceTypeNormalizer.AliasKeys.ToList();
        var conditions = string.Join(" OR ",
            aliasKeys.Select((_, i) =>
                $"CONTAINS(LOWER(c.properties.raceType), @t{i}) OR CONTAINS(LOWER(c.properties.typeLocal), @t{i})"));

        var query = new QueryDefinition(
            $"SELECT * FROM c WHERE c.kind = @kind AND ({conditions})")
            .WithParameter("@kind", FeatureKinds.Race);

        for (var i = 0; i < aliasKeys.Count; i++)
            query = query.WithParameter($"@t{i}", aliasKeys[i].ToLowerInvariant());

        var races = (await raceCollectionClient.ExecuteQueryAsync<StoredFeature>(query)).ToList();
        logger.LogInformation("Found {Count} races with stale type tokens", races.Count);

        var updated = 0;
        var skipped = 0;
        var batch = new List<(string Id, PartitionKey PartitionKey, IReadOnlyList<PatchOperation> Operations)>();
        const int batchSize = 20;

        foreach (var race in races)
        {
            string? rawType = race.Properties.TryGetValue("typeLocal", out var tl) ? tl?.ToString() : null;
            rawType ??= race.Properties.TryGetValue("raceType", out var rt) ? rt?.ToString() : null;

            var newNormalized = RaceTypeNormalizer.NormalizeRaceType(rawType);
            string? oldNormalized = race.Properties.TryGetValue("raceType", out var old) ? old?.ToString() : null;

            if (newNormalized == oldNormalized)
            {
                skipped++;
                continue;
            }

            logger.LogInformation("Race {Id}: \"{Old}\" -> \"{New}\"", race.Id, oldNormalized ?? "(null)", newNormalized ?? "(null)");

            var pk = new PartitionKeyBuilder().Add(race.X).Add(race.Y).Build();
            PatchOperation[] patchOps = newNormalized != null
                ? [PatchOperation.Set("/properties/raceType", newNormalized)]
                : [PatchOperation.Remove("/properties/raceType")];

            batch.Add((race.Id, pk, patchOps));
            updated++;

            if (batch.Count >= batchSize)
            {
                await raceCollectionClient.PatchDocuments(batch);
                batch.Clear();
                logger.LogInformation("Progress: {Updated} updated, {Skipped} skipped of {Total}", updated, skipped, races.Count);
            }
        }

        if (batch.Count > 0)
            await raceCollectionClient.PatchDocuments(batch);

        logger.LogInformation("Re-normalized race types: {Updated}/{Total} updated", updated, races.Count);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new { updated, total = races.Count });
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
}
