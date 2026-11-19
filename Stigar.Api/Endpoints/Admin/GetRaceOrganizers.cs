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
    IOrganizerStore organizerStore,
    IConfiguration configuration)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/races/organizers",
            (HttpContext context, [FromServices] GetRaceOrganizers endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var query = context.Request.Query;
        var search = query["search"].ToString().Trim();
        var source = query["source"].ToString().Trim();
        var hasDiscovery = ParseOptionalBool(query["hasDiscovery"].ToString());
        var hasScraper = ParseOptionalBool(query["hasScraper"].ToString());
        var limit = int.TryParse(query["limit"].ToString(), out var parsedLimit) && parsedLimit > 0 ? parsedLimit : 200;

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

        return TypedResults.Ok(results);
    }

    private static bool? ParseOptionalBool(string? raw)
        => bool.TryParse(raw, out var parsed) ? parsed : null;
}
