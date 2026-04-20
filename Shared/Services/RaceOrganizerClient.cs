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
                if (host == "runsignup.com")
                    path = NormalizeRunSignupPath(path);
                else if (host == "my.raceresult.com")
                    path = NormalizeRaceResultPath(path);
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
            remaining = remaining.Skip(remaining.Count - 3).ToList();

        return string.Join("/", new[] { "Race" }.Concat(remaining));
    }

    private static string NormalizeRaceResultPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
            return path;

        return int.TryParse(segments[0], out _) ? segments[0] : path;
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
