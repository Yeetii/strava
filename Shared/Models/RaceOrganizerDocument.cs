namespace Shared.Models;

/// <summary>
/// Working document for a race organizer domain. One document per organizer (domain key).
/// Discovery sources append hints under <c>Discovery[source]</c>, scrapers under <c>Scrapers[scraper]</c>.
/// An assembly worker reads this to produce final <see cref="StoredFeature"/> documents in the races container.
/// </summary>
public class RaceOrganizerDocument : IDocument
{
    /// <summary>Clean domain key, e.g. "nighttrailrun.se".</summary>
    public required string Id { get; set; }

    /// <summary>Canonical URL for this organizer (with scheme).</summary>
    public required string Url { get; set; }

    /// <summary>
    /// Per-source discovery hints. Keys: "utmb", "tracedetrail", "runagain", "loppkartan", "manual".
    /// Each source may contribute multiple discoveries (e.g. RunAgain finds two events on the same domain).
    /// </summary>
    public Dictionary<string, List<SourceDiscovery>>? Discovery { get; set; }

    /// <summary>Per-scraper output. Keys: "utmb", "itra", "bfs", "tracedetrail".</summary>
    public Dictionary<string, ScraperOutput>? Scrapers { get; set; }

    public string? LastAssembledUtc { get; set; }

    /// <summary>Highest race slot index from the last successful assembly run. Avoids a fan-out query when expiring superseded slots.</summary>
    public int? LastMaxSlotIndex { get; set; }

    /// <summary>Per-slot content hashes from the last assembly. Key: slot key e.g. "nighttrailrun.se-0".</summary>
    public Dictionary<string, RaceSlotHashes>? AssemblyHashes { get; set; }
}

/// <summary>
/// Content hashes for one assembled race slot, stored on the organizer document to
/// detect geometry and property changes across re-assembly runs.
/// </summary>
public class RaceSlotHashes
{
    public string? PropertiesHash { get; set; }
    public string? GeometryHash { get; set; }
}

/// <summary>
/// Discovery metadata from a single source. Each source overwrites its own list freely.
/// </summary>
public class SourceDiscovery
{
    public required string DiscoveredAtUtc { get; set; }

    // ── Event-level metadata ──────────────────────────────────────────────
    public string? Name { get; set; }
    public string? Date { get; set; }
    public double? Latitude { get; set; }
    public double? Longitude { get; set; }
    public string? Distance { get; set; }
    public double? ElevationGain { get; set; }
    public string? Country { get; set; }
    public string? Location { get; set; }
    public string? RaceType { get; set; }
    public string? ImageUrl { get; set; }
    public string? LogoUrl { get; set; }
    public string? Organizer { get; set; }
    public string? Description { get; set; }
    public string? StartFee { get; set; }
    public string? Currency { get; set; }
    public string? County { get; set; }
    public string? TypeLocal { get; set; }
    public bool? RegistrationOpen { get; set; }
    public Dictionary<string, string>? ExternalIds { get; set; }

    /// <summary>Source-specific URLs (e.g. UTMB race pages, RunAgain page).</summary>
    public List<string>? SourceUrls { get; set; }

    // ── ITRA-specific ───────────────────────────────────────────────────--
    public int? ItraPoints { get; set; }
    public bool? ItraNationalLeague { get; set; }

    // ── UTMB-specific ─────────────────────────────────────────────────────
    public List<string>? Playgrounds { get; set; }
    public int? RunningStones { get; set; }
    public string? UtmbWorldSeriesCategory { get; set; }
}

/// <summary>
/// Output from a single scraper run, stored under <c>Scrapers[scraperKey]</c>.
/// </summary>
public class ScraperOutput
{
    public required string ScrapedAtUtc { get; set; }
    public List<ScrapedRouteOutput>? Routes { get; set; }
    public string? WebsiteUrl { get; set; }
    public string? ImageUrl { get; set; }
    public string? LogoUrl { get; set; }
    public string? ExtractedName { get; set; }
    public string? ExtractedDate { get; set; }
    public string? StartFee { get; set; }
    public string? Currency { get; set; }
}

/// <summary>
/// A single scraped route stored in the working document.
/// </summary>
public class ScrapedRouteOutput
{
    /// <summary>Coordinates as [lng, lat] pairs.</summary>
    public List<double[]>? Coordinates { get; set; }
    public string? SourceUrl { get; set; }
    public string? Name { get; set; }
    public string? Distance { get; set; }
    public double? ElevationGain { get; set; }
    public string? GpxUrl { get; set; }
    public string? ImageUrl { get; set; }
    public string? LogoUrl { get; set; }
    public string? Date { get; set; }
    public string? StartFee { get; set; }
    public string? Currency { get; set; }

    /// <summary>
    /// Where GPX coordinates came from: <c>dropbox</c>, <c>google_drive</c>, <c>internal_gpx</c>, <c>external_gpx</c>.
    /// </summary>
    public string? GpxSource { get; set; }
}
