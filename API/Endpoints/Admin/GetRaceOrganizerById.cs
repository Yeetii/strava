using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using API.Http;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public record AdminRaceDiscoveryEntry(
    string Source,
    string? Name,
    string? Date,
    string? Distance,
    string? RaceType,
    string? WebsiteUrl,
    string? ImageUrl,
    string? StartFee,
    string? Currency,
    IReadOnlyList<string> SourceUrls);

public record AdminRaceScraperEntry(
    string Scraper,
    string? WebsiteUrl,
    string? ImageUrl,
    string? StartFee,
    string? Currency,
    int RouteCount);

public record AdminRaceOrganizerDetail(
    string OrganizerKey,
    string Url,
    string? LastScrapedUtc,
    string? LastAssembledUtc,
    IReadOnlyDictionary<string, IReadOnlyList<AdminRaceDiscoveryEntry>> Discovery,
    IReadOnlyDictionary<string, AdminRaceScraperEntry> Scrapers);

public class GetRaceOrganizerById(
    IOrganizerStore organizerStore,
    IConfiguration configuration)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/races/organizers/{organizerKey}",
            (HttpContext context, [FromServices] GetRaceOrganizerById endpoint, string organizerKey) => endpoint.Handle(context, organizerKey, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string organizerKey, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var doc = await organizerStore.GetByIdAsync(organizerKey, cancellationToken);
        if (doc is null)
            return TypedResults.NotFound();

        var discovery = (doc.Discovery ?? [])
            .ToDictionary(
                kv => kv.Key,
                kv => (IReadOnlyList<AdminRaceDiscoveryEntry>)kv.Value.Select(entry => new AdminRaceDiscoveryEntry(
                    kv.Key,
                    entry.Name,
                    entry.Date,
                    entry.Distance,
                    entry.RaceType,
                    GetDiscoveryWebsiteUrl(entry),
                    entry.ImageUrl,
                    entry.StartFee,
                    entry.Currency,
                    entry.SourceUrls ?? [])).ToArray(),
                StringComparer.OrdinalIgnoreCase);

        var scrapers = (doc.Scrapers ?? [])
            .ToDictionary(
                kv => kv.Key,
                kv => new AdminRaceScraperEntry(
                    kv.Key,
                    kv.Value.WebsiteUrl,
                    kv.Value.ImageUrl,
                    kv.Value.StartFee,
                    kv.Value.Currency,
                    kv.Value.Routes?.Count ?? 0),
                StringComparer.OrdinalIgnoreCase);

        var detail = new AdminRaceOrganizerDetail(
            doc.Id,
            doc.Url,
            doc.LastScrapedUtc,
            doc.LastAssembledUtc,
            discovery,
            scrapers);

        return TypedResults.Ok(detail);
    }

    private static string? GetDiscoveryWebsiteUrl(SourceDiscovery entry)
    {
        if (entry.SourceUrls is { Count: > 0 })
        {
            var preferred = entry.SourceUrls.FirstOrDefault(url =>
                Uri.TryCreate(url, UriKind.Absolute, out var uri)
                && !uri.Host.Contains("mittlopp.se", StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrWhiteSpace(preferred))
                return preferred;

            return entry.SourceUrls[0];
        }

        return null;
    }
}
