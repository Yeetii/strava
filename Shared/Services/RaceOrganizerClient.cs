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

    /// <summary>
    /// Derives a Cosmos-safe organizer key from a URL. For regular domains returns just the host
    /// (stripped of "www."). For platform hosts (Facebook, TraceDeTrail, etc.) keeps the
    /// first meaningful path segments, using "~" instead of "/" (which Cosmos disallows in IDs).
    /// </summary>
    public static string DeriveOrganizerKey(Uri url) => OrganizerUrlRules.DeriveOrganizerKey(url);

    public async Task<List<string>> GetIdsDueForAutomaticScrapeAsync(
        DateTime cutoffUtc,
        CancellationToken cancellationToken = default)
    {
        const string sqlQuery = "SELECT VALUE c.id FROM c WHERE NOT IS_DEFINED(c.lastScrapedUtc) OR IS_NULL(c.lastScrapedUtc) OR c.lastScrapedUtc < @cutoffUtc";

        List<string> ids = [];
        var queryDefinition = new QueryDefinition(sqlQuery)
            .WithParameter("@cutoffUtc", cutoffUtc.ToString("o"));

        using FeedIterator<string> resultSet = _container.GetItemQueryIterator<string>(queryDefinition);
        while (resultSet.HasMoreResults)
        {
            FeedResponse<string> response = await resultSet.ReadNextAsync(cancellationToken);
            ids.AddRange(response);
        }

        return ids;
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

    public async Task PatchScraperPropertiesAsync(
        string organizerKey,
        string scraperKey,
        ScraperOutput output,
        CancellationToken cancellationToken = default)
    {
        var pk = new PartitionKey(organizerKey);
        var basePath = $"/scrapers/{scraperKey}";

        await _container.PatchItemAsync<RaceOrganizerDocument>(
            organizerKey,
            pk,
            [
                PatchOperation.Set($"{basePath}/scrapedAtUtc", output.ScrapedAtUtc),
                PatchOperation.Set($"{basePath}/websiteUrl", output.WebsiteUrl),
                PatchOperation.Set($"{basePath}/imageUrl", output.ImageUrl),
                PatchOperation.Set($"{basePath}/logoUrl", output.LogoUrl),
                PatchOperation.Set($"{basePath}/extractedName", output.ExtractedName),
                PatchOperation.Set($"{basePath}/extractedDate", output.ExtractedDate),
                PatchOperation.Set($"{basePath}/startFee", output.StartFee),
                PatchOperation.Set($"{basePath}/currency", output.Currency),
            ],
            cancellationToken: cancellationToken);
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

    /// <summary>
    /// Patches assembly metadata on an existing organizer document:
    /// <c>lastAssembledUtc</c>, <c>lastMaxSlotIndex</c>, and <c>assemblyHashes</c>.
    /// </summary>
    public async Task PatchLastAssembledAsync(
        string organizerKey,
        int? maxSlotIndex,
        Dictionary<string, RaceSlotHashes>? assemblyHashes,
        CancellationToken cancellationToken = default)
    {
        var pk = new PartitionKey(organizerKey);
        var ops = new List<PatchOperation>
        {
            PatchOperation.Add("/lastAssembledUtc", DateTime.UtcNow.ToString("o")),
        };
        if (maxSlotIndex.HasValue)
            ops.Add(PatchOperation.Add("/lastMaxSlotIndex", maxSlotIndex.Value));
        if (assemblyHashes is not null)
            ops.Add(PatchOperation.Add("/assemblyHashes", assemblyHashes));
        await _container.PatchItemAsync<RaceOrganizerDocument>(
            organizerKey, pk, ops, cancellationToken: cancellationToken);
    }

    public async Task PatchLastScrapedAsync(
        string organizerKey,
        string lastScrapedUtc,
        Dictionary<string, ScraperOutputHashes>? scraperHashes,
        CancellationToken cancellationToken = default)
    {
        var pk = new PartitionKey(organizerKey);
        var ops = new List<PatchOperation>
        {
            PatchOperation.Set("/lastScrapedUtc", lastScrapedUtc),
        };
        if (scraperHashes is not null)
            ops.Add(PatchOperation.Set("/scraperHashes", scraperHashes));

        await _container.PatchItemAsync<RaceOrganizerDocument>(
            organizerKey, pk, ops, cancellationToken: cancellationToken);
    }
}
