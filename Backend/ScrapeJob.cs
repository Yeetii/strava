using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Shared.Services;

namespace Backend;

// Flat message enqueued onto the unified scrapeRace service bus queue.
// Each source has its own typed URL field; the worker tries scrapers in priority order
// (UTMB → ITRA/TraceDeTrail → Runagain → BFS/Loppkartan) and uses the first result.
// Any combination of source URLs may be set; all null → point fallback using lat/lng.
public record ScrapeJob(
    string? Name = null,
    IReadOnlyDictionary<string, string>? ExternalIds = null, // issuer/authority → opaque ID (e.g. "utmb" → "133", "tracedetrail" → "12345")
    string? Distance = null,     // pre-formatted, e.g. "50 km" or "50 km, 25 km"
    double? ElevationGain = null,
    string? Country = null,
    string? Location = null,
    string? RaceType = null,
    bool? RegistrationOpen = null,
    string? Date = null,
    string? ImageUrl = null,
    string? LogoUrl = null,
    double? Latitude = null,
    double? Longitude = null,
    IReadOnlyList<string>? Playgrounds = null,
    int? ItraPoints = null,
    int? RunningStones = null,
    string? UtmbWorldSeriesCategory = null,
    string? County = null,
    string? TypeLocal = null,
    string? Organizer = null,
    string? Description = null,
    string? StartFee = null,
    string? Currency = null,
    // Per-source URLs (null = source not available for this race).
    Uri? UtmbUrl = null,
    IReadOnlyList<Uri>? TraceDeTrailItraUrls = null,
    Uri? TraceDeTrailEventUrl = null,
    Uri? ItraEventPageUrl = null,
    bool? ItraNationalLeague = null,
    Uri? RunagainUrl = null,
    Uri? WebsiteUrl = null,         // generic race website (e.g. from Loppkartan)
    Uri? BetrailUrl = null)
{
    /// <summary>
    /// Converts this ScrapeJob to a <see cref="SourceDiscovery"/> for storage in a working document.
    /// </summary>
    public Shared.Models.SourceDiscovery ToSourceDiscovery() => new()
    {
        DiscoveredAtUtc = DateTime.UtcNow.ToString("o"),
        Name = Name,
        Date = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(Date) ?? Date,
        Latitude = Latitude,
        Longitude = Longitude,
        Distance = Distance,
        ElevationGain = ElevationGain,
        Country = RaceScrapeDiscovery.NormalizeCountryToIso2(Country) ?? Country,
        Location = Location,
        RaceType = RaceScrapeDiscovery.NormalizeRaceType(RaceType) ?? RaceType,
        ImageUrl = ImageUrl,
        LogoUrl = LogoUrl,
        Organizer = Organizer,
        Description = Description,
        StartFee = StartFee,
        Currency = Currency,
        County = County,
        TypeLocal = TypeLocal,
        RegistrationOpen = RegistrationOpen,
        ExternalIds = ExternalIds is { Count: > 0 } ? new Dictionary<string, string>(ExternalIds) : null,
        SourceUrls = GetSourceUrls(),
        ItraPoints = ItraPoints,
        ItraNationalLeague = ItraNationalLeague,
        Playgrounds = Playgrounds is { Count: > 0 } ? [.. Playgrounds] : null,
        RunningStones = RunningStones,
        UtmbWorldSeriesCategory = UtmbWorldSeriesCategory,
    };

    private List<string>? GetSourceUrls()
    {
        var urls = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void Add(Uri? uri)
        {
            if (uri is null) return;
            var absolute = uri.AbsoluteUri;
            if (seen.Add(absolute))
                urls.Add(absolute);
        }

        Add(UtmbUrl);
        if (TraceDeTrailItraUrls is { Count: > 0 })
        {
            foreach (var u in TraceDeTrailItraUrls)
                Add(u);
        }
        Add(TraceDeTrailEventUrl);
        Add(ItraEventPageUrl);
        Add(RunagainUrl);
        Add(WebsiteUrl);
        Add(BetrailUrl);

        return urls.Count > 0 ? urls : null;
    }
}

public record TraceDeTrailTraceData(
    IReadOnlyList<(double Lng, double Lat)> Points,
    double? TotalDistanceKm,
    double? ElevationGain);

public record TraceDeTrailCourseInfo(
    string? Name,
    string? Distance,
    double? ElevationGain,
    int? ItraPoints,
    int? TraceId);
