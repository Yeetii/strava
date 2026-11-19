using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Stigar.Api.Http;
using Stigar.Core.Models;
using Stigar.Core.Services;

namespace Stigar.Api.Endpoints.Admin;

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
    IOrganizerStore organizerStore,
    IConfiguration configuration)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/races/assembled",
            (HttpContext context, [FromServices] GetAssembledRaces endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var query = context.Request.Query;
        var organizerKey = query["organizerKey"].ToString().Trim();
        var search = query["search"].ToString().Trim();
        var raceType = query["raceType"].ToString().Trim();
        var limit = int.TryParse(query["limit"].ToString(), out var parsedLimit) && parsedLimit > 0 ? parsedLimit : 200;

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

        return TypedResults.Ok(results);
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
