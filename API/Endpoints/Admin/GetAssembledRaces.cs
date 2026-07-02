using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public record AdminAssembledRaceListItem(
    string FeatureId,
    string StoredId,
    string OrganizerKey,
    string? Name,
    string? Date,
    string? Distance,
    string? RaceType,
    string? Website,
    string? Image,
    string? StartFee,
    string? Currency,
    IReadOnlyList<string> Sources);

public class GetAssembledRaces(
    BlobOrganizerStore organizerStore,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"], Summary = "List assembled race features.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "organizerKey", In = ParameterLocation.Query, Type = typeof(string), Required = false)]
    [OpenApiParameter(name: "search", In = ParameterLocation.Query, Type = typeof(string), Required = false)]
    [OpenApiParameter(name: "raceType", In = ParameterLocation.Query, Type = typeof(string), Required = false)]
    [OpenApiParameter(name: "limit", In = ParameterLocation.Query, Type = typeof(int), Required = false)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IReadOnlyList<AdminAssembledRaceListItem>))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetAssembledRaces))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/races/assembled")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
        var organizerKey = query["organizerKey"]?.Trim();
        var search = query["search"]?.Trim();
        var raceType = query["raceType"]?.Trim();
        var limit = int.TryParse(query["limit"], out var parsedLimit) && parsedLimit > 0 ? parsedLimit : 200;

        var results = new List<AdminAssembledRaceListItem>();
        await foreach (var item in organizerStore.StreamAssembledRacesAsync(organizerKey, maxConcurrency: 32, cancellationToken))
        {
            var feature = item.Race;
            if (StoredFeature.IsPointerDocument(feature))
                continue;

            var name = TryGetString(feature.Properties, "name");
            var featureRaceType = TryGetString(feature.Properties, "raceType");
            if (!string.IsNullOrWhiteSpace(search)
                && !(name?.Contains(search, StringComparison.OrdinalIgnoreCase) ?? false)
                && !feature.LogicalId.Contains(search, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(raceType)
                && !(featureRaceType?.Contains(raceType, StringComparison.OrdinalIgnoreCase) ?? false))
            {
                continue;
            }

            results.Add(new AdminAssembledRaceListItem(
                feature.LogicalId,
                feature.Id,
                item.OrganizerKey,
                name,
                TryGetString(feature.Properties, "date"),
                TryGetString(feature.Properties, "distance"),
                featureRaceType,
                TryGetString(feature.Properties, "website"),
                TryGetString(feature.Properties, "image"),
                TryGetString(feature.Properties, "startFee"),
                TryGetString(feature.Properties, "currency"),
                TryGetStringList(feature.Properties, "sources")));

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

    private static string? TryGetString(IDictionary<string, dynamic> props, string key)
        => props.TryGetValue(key, out var value) ? value?.ToString() : null;

    private static IReadOnlyList<string> TryGetStringList(IDictionary<string, dynamic> props, string key)
    {
        if (!props.TryGetValue(key, out var value) || value is null)
            return [];

        if (value is IEnumerable<object> objects)
            return objects.Select(x => x?.ToString()).Where(x => !string.IsNullOrWhiteSpace(x)).Cast<string>().ToArray();

        return value is IEnumerable<string> strings ? strings.ToArray() : [];
    }
}
