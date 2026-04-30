using Shared.Models;

namespace Shared.Services;

public sealed class OrganizerBlobIdentity
{
    public required string Id { get; init; }
    public required string Url { get; init; }
}

public sealed class OrganizerBlobMetadataDocument
{
    public required string Id { get; init; }
    public required string Url { get; init; }
    public Dictionary<string, List<SourceDiscovery>>? Discovery { get; init; }
    public Dictionary<string, OrganizerBlobScraperMetadata>? Scrapers { get; init; }
    public string? LastScrapedUtc { get; init; }
    public Dictionary<string, ScraperOutputHashes>? ScraperHashes { get; init; }
    public string? LastAssembledUtc { get; init; }
    public int? LastMaxSlotIndex { get; init; }
    public Dictionary<string, RaceSlotHashes>? AssemblyHashes { get; init; }
}

public sealed class OrganizerBlobScraperMetadata
{
    public required string ScrapedAtUtc { get; init; }
    public List<OrganizerBlobRouteMetadata>? Routes { get; init; }
    public string? WebsiteUrl { get; init; }
    public string? ImageUrl { get; init; }
    public string? LogoUrl { get; init; }
    public string? ExtractedName { get; init; }
    public string? ExtractedDate { get; init; }
    public string? StartFee { get; init; }
    public string? Currency { get; init; }
}

public sealed class OrganizerBlobRouteMetadata
{
    public string? SourceUrl { get; init; }
    public string? Name { get; init; }
    public string? Distance { get; init; }
    public double? ElevationGain { get; init; }
    public string? GpxUrl { get; init; }
    public string? ImageUrl { get; init; }
    public string? LogoUrl { get; init; }
    public string? Date { get; init; }
    public string? StartFee { get; init; }
    public string? Currency { get; init; }
    public string? GpxSource { get; init; }
}