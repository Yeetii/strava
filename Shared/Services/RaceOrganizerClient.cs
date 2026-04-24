using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Models;

namespace Shared.Services;

public class RaceOrganizerClient(Container container, ILoggerFactory loggerFactory)
    : CollectionClient<RaceOrganizerDocument>(container, loggerFactory)
{
    private readonly Container _container = container;
    private readonly ILogger _logger = loggerFactory.CreateLogger<RaceOrganizerClient>();

    // Hosts where the path is significant (the domain alone doesn't identify an organizer).
    private static readonly HashSet<string> PlatformHosts = new(StringComparer.OrdinalIgnoreCase)
    {
        "facebook.com",
        "instagram.com",
        "tracedetrail.fr",
        "nestelop.no",
        "runsignup.com",
        "ultrasignup.com",
        "my.raceresult.com",
        "raceroster.com",
        "welcu.com",
        "betrail.run",
        "itra.run",
        "sites.google.com",
        "runagain.com",
        "klikego.com",
        "mp.weixin.qq.com",
        "fr.milesrepublic.com"
    };
    /// <summary>
    /// Derives a Cosmos-safe organizer key from a URL. For regular domains returns just the host
    /// (stripped of "www."). For platform hosts (Facebook, TraceDeTrail, etc.) keeps the
    /// first meaningful path segments, using "~" instead of "/" (which Cosmos disallows in IDs).
    /// </summary>
    public static string DeriveOrganizerKey(Uri url)
    {
        var host = url.Host.ToLowerInvariant();
        if (host.StartsWith("www.", StringComparison.Ordinal))
            host = host[4..];

        if (PlatformHosts.Contains(host))
        {
            var path = url.AbsolutePath.Trim('/');
            if (!string.IsNullOrEmpty(path))
            {
                if (host == "facebook.com")
                    path = NormalizeFacebookPath(path, url.Query);
                else if (host == "runsignup.com")
                    path = NormalizeRunSignupPath(path);
                else if (host == "ultrasignup.com")
                    path = NormalizeUltraSignupPath(path, url.Query);
                else if (host == "my.raceresult.com" || host == "welcu.com")
                    path = NormalizeFirstPathSegment(path);
                else if (host == "raceroster.com")
                    path = NormalizeRaceRosterPath(path);
                else if (host == "betrail.run")
                    path = NormalizeBeTrailPath(path);
                else if (host == "itra.run")
                    path = NormalizeItraPath(path);
                else if (host == "sites.google.com")
                    path = NormalizeSitesGooglePath(path);
                else if (host == "klikego.com")
                    path = NormalizeKlikegoPath(path);
                return $"{host}~{path.Replace('/', '~')}";
            }
        }

        return host;
    }

    private static string NormalizeRunSignupPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
            return path;

        if (!segments[0].Equals("Race", StringComparison.OrdinalIgnoreCase))
            return path;

        var remaining = segments.Skip(1).ToList();
        while (remaining.Count > 0 && remaining[0].Equals("Events", StringComparison.OrdinalIgnoreCase))
            remaining.RemoveAt(0);

        if (remaining.Count > 3)
            remaining = [.. remaining.Skip(remaining.Count - 3)];

        return string.Join("/", ["Race", ..remaining]);
    }

    // UltraSignup: /register.aspx?did=<id>, /results_event.aspx?did=<id>, /entrants_event.aspx?did=<id>
    // all describe the same event — the race identity lives entirely in the `did` query param.
    // Collapse every .aspx tab to `register.aspx?did=<id>` so one organizer document covers them all.
    // Paths without `did` (e.g. modern /race/<slug> style) fall through unchanged.
    private static string NormalizeUltraSignupPath(string path, string query)
    {
        if (!path.EndsWith(".aspx", StringComparison.OrdinalIgnoreCase) || string.IsNullOrEmpty(query))
            return path;

        var did = GetQueryValue(query, "did");
        return string.IsNullOrWhiteSpace(did) ? path : $"register.aspx?did={did}";
    }

    private static string NormalizeFirstPathSegment(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
            return path;

        return segments[0];
    }

    private static string NormalizeRaceRosterPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 4)
            return path;

        if (!segments[0].Equals("events", StringComparison.OrdinalIgnoreCase))
            return path;

        return string.Join('/', segments.Take(4));
    }

    // Klikego registration pages: /inscription/<event-slug>/<discipline>/<registration-id>?<params>
    // → keep only inscription/<event-slug>. The discipline and numeric registration id identify
    // one sub-race on the event, but the event itself is identified purely by the slug.
    private static string NormalizeKlikegoPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var idx = Array.FindIndex(segments, s => s.Equals("inscription", StringComparison.OrdinalIgnoreCase));
        if (idx < 0 || idx + 1 >= segments.Length)
            return path;

        return $"inscription/{segments[idx + 1]}";
    }

    // BeTrail race pages: /race/<slug>/[<year>/[<tab>]] → keep only race/<slug>.
    // Accepts an optional language prefix (/en/, /fr/, /nl/, …) which is dropped so that
    // translations of the same page collapse to a single organizer key.
    private static string NormalizeBeTrailPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var raceIdx = Array.FindIndex(segments, s => s.Equals("race", StringComparison.OrdinalIgnoreCase));
        if (raceIdx < 0 || raceIdx + 1 >= segments.Length)
            return path;

        return $"race/{segments[raceIdx + 1]}";
    }

    // ITRA race pages: /Races/RaceDetails/<name>/<year>/<id> → keep only /Races/RaceDetails/<name>.
    // The year and ITRA race-id trail uniquely identify an edition; we group all editions under the name.
    private static string NormalizeItraPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 3)
            return path;

        if (!segments[0].Equals("Races", StringComparison.OrdinalIgnoreCase)
            || !segments[1].Equals("RaceDetails", StringComparison.OrdinalIgnoreCase))
        {
            return path;
        }

        return string.Join('/', segments.Take(3));
    }

    // Google Sites: collapse to the site root regardless of hosting flavour.
    //   /site/<name>/<page>…        → site/<name>
    //   /view/<name>/<page>…        → view/<name>
    //   /<custom-domain>/<name>/…   → <custom-domain>       (workspace-hosted: the domain itself is the identity)
    private static string NormalizeSitesGooglePath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
            return path;

        var first = segments[0];
        if (first.Equals("site", StringComparison.OrdinalIgnoreCase)
            || first.Equals("view", StringComparison.OrdinalIgnoreCase))
        {
            return segments.Length >= 2 ? $"{first}/{segments[1]}" : first;
        }

        // Workspace / custom-domain sites: the first segment is a domain (contains a dot)
        // and uniquely identifies the organizer on its own.
        return first;
    }

    private static string NormalizeFacebookPath(string path, string query)
    {
        if (!path.Equals("profile.php", StringComparison.OrdinalIgnoreCase) || string.IsNullOrEmpty(query))
            return path;

        var id = GetQueryValue(query, "id");
        return string.IsNullOrWhiteSpace(id) ? path : $"{path}?id={id}";
    }

    private static string? GetQueryValue(string query, string key)
    {
        var trimmed = query.StartsWith("?", StringComparison.Ordinal) ? query[1..] : query;
        foreach (var part in trimmed.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var separatorIndex = part.IndexOf('=');
            if (separatorIndex < 0)
                continue;

            var name = WebUtility.UrlDecode(part[..separatorIndex]);
            if (!name.Equals(key, StringComparison.OrdinalIgnoreCase))
                continue;

            return WebUtility.UrlDecode(part[(separatorIndex + 1)..]);
        }

        return null;
    }

    /// <summary>
    /// Writes discovery data for a single source. Creates the document if it doesn't exist,
    /// otherwise patches just the <c>/discovery/{source}</c> path, replacing the entire list for that source.
    /// </summary>
    public async Task WriteDiscoveryAsync(
        string organizerKey,
        string canonicalUrl,
        string source,
        List<SourceDiscovery> discoveries,
        CancellationToken cancellationToken = default)
    {
        var pk = new PartitionKey(organizerKey);

        try
        {
            // Try patch first (document already exists).
            await _container.PatchItemAsync<RaceOrganizerDocument>(
                organizerKey,
                pk,
                [PatchOperation.Set($"/discovery/{source}", discoveries)],
                cancellationToken: cancellationToken);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Document doesn't exist — create it with the discovery data.
            var doc = new RaceOrganizerDocument
            {
                Id = organizerKey,
                Url = canonicalUrl,
                Discovery = new Dictionary<string, List<SourceDiscovery>> { [source] = discoveries },
                Scrapers = new Dictionary<string, ScraperOutput>()
            };
            await _container.CreateItemAsync(doc, pk, cancellationToken: cancellationToken);
        }
    }

    /// <summary>
    /// Patches scraper output for a single scraper key onto an existing organizer document.
    /// </summary>
    public async Task WriteScraperOutputAsync(
        string organizerKey,
        string scraperKey,
        ScraperOutput output,
        CancellationToken cancellationToken = default)
    {
        var pk = new PartitionKey(organizerKey);
        try
        {
            await _container.PatchItemAsync<RaceOrganizerDocument>(
                organizerKey, pk,
                [PatchOperation.Set($"/scrapers/{scraperKey}", output)],
                cancellationToken: cancellationToken);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.BadRequest)
        {
            // /scrapers is null on the document — initialize it with this entry.
            await _container.PatchItemAsync<RaceOrganizerDocument>(
                organizerKey, pk,
                [PatchOperation.Set("/scrapers", new Dictionary<string, ScraperOutput> { [scraperKey] = output })],
                cancellationToken: cancellationToken);
        }
    }

    /// <summary>
    /// Writes discovery data for multiple organizers from the same source in one batch.
    /// </summary>
    public async Task WriteDiscoveriesAsync(
        string source,
        IReadOnlyList<(string OrganizerKey, string CanonicalUrl, List<SourceDiscovery> Discoveries)> items,
        CancellationToken cancellationToken = default)
    {
        foreach (var (organizerKey, url, discoveries) in items)
        {
            try
            {
                await WriteDiscoveryAsync(organizerKey, url, source, discoveries, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Failed to write discovery for {Source}/{OrganizerKey}", source, organizerKey);
            }
        }
    }

    public async Task<HashSet<string>> FetchKnownTraceDeTrailIdsAsync(CancellationToken cancellationToken = default)
    {
        var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var tracedetrailQuery = new QueryDefinition(
            "SELECT VALUE s.externalIds.tracedetrailEventId FROM c JOIN s IN c.discovery.tracedetrail " +
            "WHERE IS_DEFINED(s.externalIds.tracedetrailEventId)");
        var tracedetrailIds = await ExecuteQueryAsync<string>(tracedetrailQuery, cancellationToken: cancellationToken);
        foreach (var id in tracedetrailIds.Where(id => !string.IsNullOrWhiteSpace(id)))
            ids.Add(id!);

        var itraQuery = new QueryDefinition(
            "SELECT VALUE s.externalIds.itraEventId FROM c JOIN s IN c.discovery.tracedetrail " +
            "WHERE IS_DEFINED(s.externalIds.itraEventId)");
        var itraIds = await ExecuteQueryAsync<string>(itraQuery, cancellationToken: cancellationToken);
        foreach (var id in itraIds.Where(id => !string.IsNullOrWhiteSpace(id)))
            ids.Add(id!);

        return ids;
    }

    /// <summary>
    /// Patches the <c>lastAssembledUtc</c> field on an existing organizer document.
    /// </summary>
    public async Task PatchLastAssembledAsync(
        string organizerKey,
        CancellationToken cancellationToken = default)
    {
        var pk = new PartitionKey(organizerKey);
        await _container.PatchItemAsync<RaceOrganizerDocument>(
            organizerKey, pk,
            [PatchOperation.Set("/lastAssembledUtc", DateTime.UtcNow.ToString("o"))],
            cancellationToken: cancellationToken);
    }
}
