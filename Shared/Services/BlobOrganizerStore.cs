using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Logging;
using Shared.Models;

namespace Shared.Services;

/// <summary>
/// Blob-backed replacement for <see cref="RaceOrganizerClient"/>.
/// Each organizer is stored as a single JSON blob at <c>{organizerKey}/organizer.json</c>.
/// Concurrent modifications use ETag-based optimistic concurrency with a short retry loop.
/// </summary>
public class BlobOrganizerStore(BlobContainerClient container, ILoggerFactory loggerFactory)
{
    private const string BlobSuffix = "/organizer.json";
    private const string RedirectSuffix = "/redirect.txt";
    private const string AssembledRaceFolder = "/races/";
    private const string MetaLastScrapedUtc = "lastscrapedutc"; // blob metadata keys are lowercase
    private const string MetaTransparencyHash = "transparencyhash";
    private const int MaxRetries = 5;
    private const int MaxRedirectDepth = 8;

    private readonly ILogger _logger = loggerFactory.CreateLogger<BlobOrganizerStore>();

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = true,
    };

    private static readonly JsonSerializerOptions TransparencyJsonOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };

    // ── Static helpers (drop-in for RaceOrganizerClient static members) ──

    public static string DeriveOrganizerKey(Uri url) => OrganizerUrlRules.DeriveOrganizerKey(url);

    // ── Read ─────────────────────────────────────────────────────────────

    public async Task<RaceOrganizerDocument?> GetByIdAsync(
        string organizerKey,
        CancellationToken cancellationToken = default)
    {
        organizerKey = await ResolveOrganizerKeyAsync(organizerKey, cancellationToken);
        var blob = container.GetBlobClient(BlobKey(organizerKey));
        try
        {
            var result = await blob.DownloadContentAsync(cancellationToken);
            return Deserialize(result.Value.Content);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    // ── Write (full document, no concurrency guard) ───────────────────────

    public async Task WriteAsync(
        RaceOrganizerDocument doc,
        CancellationToken cancellationToken = default)
    {
        doc.Id = await ResolveOrganizerKeyAsync(doc.Id, cancellationToken);
        var blob = container.GetBlobClient(BlobKey(doc.Id));
        await blob.UploadAsync(Serialize(doc), overwrite: true, cancellationToken);
    }

    public async Task<string> ResolveOrganizerKeyAsync(
        string organizerKey,
        CancellationToken cancellationToken = default)
    {
        var current = organizerKey.Trim();
        if (string.IsNullOrWhiteSpace(current))
            return organizerKey;

        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        while (true)
        {
            if (!visited.Add(current))
                throw new InvalidOperationException($"Organizer redirect cycle detected for '{organizerKey}'.");

            if (visited.Count > MaxRedirectDepth)
                throw new InvalidOperationException($"Organizer redirect chain exceeded {MaxRedirectDepth} hops for '{organizerKey}'.");

            var target = await GetRedirectTargetAsync(current, cancellationToken);
            if (string.IsNullOrWhiteSpace(target))
                return current;

            current = target;
        }
    }

    public async Task SetRedirectAsync(
        string sourceOrganizerKey,
        string targetOrganizerKey,
        CancellationToken cancellationToken = default)
    {
        var source = sourceOrganizerKey.Trim();
        var target = await ResolveOrganizerKeyAsync(targetOrganizerKey, cancellationToken);

        if (string.IsNullOrWhiteSpace(source))
            throw new ArgumentException("Source organizer key is required.", nameof(sourceOrganizerKey));

        if (string.IsNullOrWhiteSpace(target))
            throw new ArgumentException("Target organizer key is required.", nameof(targetOrganizerKey));

        if (string.Equals(source, target, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Source and target organizer keys must be different.");

        await DeletePrefixAsync($"{source}/", cancellationToken);

        var redirectBlob = container.GetBlobClient(RedirectBlobKey(source));
        await redirectBlob.UploadAsync(
            BinaryData.FromString(target + "\n"),
            overwrite: true,
            cancellationToken);
    }

    // ── Discovery ─────────────────────────────────────────────────────────

    public async Task WriteDiscoveryAsync(
        string organizerKey,
        string canonicalUrl,
        string source,
        List<SourceDiscovery> discoveries,
        CancellationToken cancellationToken = default)
    {
        await ModifyAsync(
            organizerKey,
            doc =>
            {
                doc.Discovery ??= [];
                doc.Discovery[source] = discoveries;
            },
            createUrl: canonicalUrl,
            cancellationToken: cancellationToken);
    }

    public async Task WriteDiscoveriesAsync(
        string source,
        IReadOnlyList<(string OrganizerKey, string CanonicalUrl, List<SourceDiscovery> Discoveries)> items,
        CancellationToken cancellationToken = default)
    {
        var merged = new Dictionary<string, (string CanonicalUrl, List<SourceDiscovery> Discoveries)>(StringComparer.OrdinalIgnoreCase);
        foreach (var (organizerKey, url, discoveries) in items)
        {
            var resolvedKey = await ResolveOrganizerKeyAsync(organizerKey, cancellationToken);
            if (merged.TryGetValue(resolvedKey, out var existing))
            {
                existing.Discoveries.AddRange(discoveries);
                merged[resolvedKey] = existing;
                continue;
            }

            merged[resolvedKey] = (url, [.. discoveries]);
        }

        foreach (var (organizerKey, item) in merged)
        {
            try
            {
                await WriteDiscoveryAsync(organizerKey, item.CanonicalUrl, source, item.Discoveries, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Failed to write discovery for {Source}/{OrganizerKey}", source, organizerKey);
            }
        }
    }

    // ── Scraper output ────────────────────────────────────────────────────

    /// <summary>Writes full scraper output (routes + metadata) for one scraper key.</summary>
    public async Task WriteScraperOutputAsync(
        string organizerKey,
        string scraperKey,
        ScraperOutput output,
        CancellationToken cancellationToken = default)
    {
        await ModifyAsync(
            organizerKey,
            doc =>
            {
                doc.Scrapers ??= [];
                doc.Scrapers[scraperKey] = output;
            },
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Writes only the metadata fields of a scraper output (no routes).
    /// With blob storage this is identical to <see cref="WriteScraperOutputAsync"/> since we
    /// always do a full read-modify-write anyway.
    /// </summary>
    public async Task PatchScraperPropertiesAsync(
        string organizerKey,
        string scraperKey,
        ScraperOutput output,
        CancellationToken cancellationToken = default)
    {
        await ModifyAsync(
            organizerKey,
            doc =>
            {
                doc.Scrapers ??= [];
                if (doc.Scrapers.TryGetValue(scraperKey, out var existing))
                {
                    // Preserve existing routes, update only metadata fields.
                    existing.ScrapedAtUtc = output.ScrapedAtUtc;
                    existing.WebsiteUrl = output.WebsiteUrl;
                    existing.ImageUrl = output.ImageUrl;
                    existing.LogoUrl = output.LogoUrl;
                    existing.ExtractedName = output.ExtractedName;
                    existing.ExtractedDate = output.ExtractedDate;
                    existing.StartFee = output.StartFee;
                    existing.Currency = output.Currency;
                }
                else
                {
                    doc.Scrapers[scraperKey] = output;
                }
            },
            cancellationToken: cancellationToken);
    }

    // ── Timestamps ────────────────────────────────────────────────────────

    public async Task PatchLastScrapedAsync(
        string organizerKey,
        string lastScrapedUtc,
        Dictionary<string, ScraperOutputHashes>? scraperHashes,
        CancellationToken cancellationToken = default)
    {
        await ModifyAsync(
            organizerKey,
            doc =>
            {
                doc.LastScrapedUtc = lastScrapedUtc;
                if (scraperHashes is not null)
                    doc.ScraperHashes = scraperHashes;
            },
            metadata: new Dictionary<string, string> { [MetaLastScrapedUtc] = lastScrapedUtc },
            cancellationToken: cancellationToken);
    }

    // ── Query ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns organizer keys for documents not scraped since <paramref name="cutoffUtc"/>.
    /// Uses blob metadata so no document bodies are downloaded.
    /// </summary>
    public async Task<List<string>> GetIdsDueForAutomaticScrapeAsync(
        DateTime cutoffUtc,
        CancellationToken cancellationToken = default)
    {
        var organizerMetadata = new List<(string OrganizerKey, IDictionary<string, string> Metadata)>();
        var redirectedKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var cutoffStr = cutoffUtc.ToString("o");

        await foreach (var item in container.GetBlobsAsync(BlobTraits.Metadata, cancellationToken: cancellationToken))
        {
            if (item.Name.EndsWith(RedirectSuffix, StringComparison.Ordinal))
            {
                redirectedKeys.Add(OrganizerKeyFromRedirectBlobName(item.Name));
                continue;
            }

            if (!item.Name.EndsWith(BlobSuffix, StringComparison.Ordinal))
                continue;

            organizerMetadata.Add((OrganizerKeyFromBlobName(item.Name), item.Metadata));
        }

        var ids = new List<string>();
        foreach (var (organizerKey, metadata) in organizerMetadata)
        {
            if (redirectedKeys.Contains(organizerKey))
                continue;

            metadata.TryGetValue(MetaLastScrapedUtc, out var lastScraped);

            // Include if never scraped or scraped before cutoff.
            if (string.IsNullOrEmpty(lastScraped) || string.Compare(lastScraped, cutoffStr, StringComparison.Ordinal) < 0)
            {
                ids.Add(organizerKey);
            }
        }

        return ids;
    }

    // ── GPX route blobs ───────────────────────────────────────────────────

    /// <summary>
    /// Uploads a raw GPX file to the given blob path within the organizers container.
    /// Used by the migration export and (optionally) by scrapers that want to store routes as
    /// separate files alongside <c>organizer.json</c>.
    /// </summary>
    public async Task UploadRouteGpxAsync(string blobPath, byte[] gpxBytes, CancellationToken cancellationToken = default)
    {
        var blob = container.GetBlobClient(blobPath);
        await blob.UploadAsync(BinaryData.FromBytes(gpxBytes), overwrite: true, cancellationToken);
    }

    /// <summary>
    /// Mirrors the latest assembled race output into per-organizer JSON blobs under
    /// <c>{organizerKey}/races/</c> for inspection and debugging.
    /// Stored geometries are marker-only to keep these transparency artifacts lightweight.
    /// </summary>
    public async Task WriteAssembledRacesAsync(
        string organizerKey,
        IReadOnlyList<StoredFeature> races,
        CancellationToken cancellationToken = default)
    {
        organizerKey = await ResolveOrganizerKeyAsync(organizerKey, cancellationToken);
        var prefix = AssembledRacePrefix(organizerKey);
        var existing = new Dictionary<string, string?>(StringComparer.Ordinal);
        var uploadTasks = new List<Task>(races.Count);

        await foreach (var item in container.GetBlobsAsync(
            traits: BlobTraits.Metadata,
            prefix: prefix,
            cancellationToken: cancellationToken))
        {
            item.Metadata.TryGetValue(MetaTransparencyHash, out var hash);
            existing[item.Name] = hash;
        }

        foreach (var race in races)
        {
            var logicalId = !string.IsNullOrWhiteSpace(race.FeatureId)
                ? race.FeatureId
                : race.LogicalId;
            var blobName = AssembledRaceBlobKey(organizerKey, logicalId);
            existing.TryGetValue(blobName, out var existingHash);
            existing.Remove(blobName);

            var marker = RaceAssembler.CreateTransparencyMarker(race);
            var payload = JsonSerializer.SerializeToUtf8Bytes(marker, TransparencyJsonOptions);
            var hash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(payload)).ToLowerInvariant();

            if (string.Equals(existingHash, hash, StringComparison.Ordinal))
            {
                continue;
            }

            var blob = container.GetBlobClient(blobName);
            uploadTasks.Add(blob.UploadAsync(
                BinaryData.FromBytes(payload),
                new BlobUploadOptions
                {
                    Metadata = new Dictionary<string, string>
                    {
                        [MetaTransparencyHash] = hash,
                    },
                },
                cancellationToken));
        }

        if (uploadTasks.Count > 0)
        {
            await Task.WhenAll(uploadTasks);
        }

        foreach (var staleBlobName in existing.Keys)
        {
            await container.DeleteBlobIfExistsAsync(staleBlobName, cancellationToken: cancellationToken);
        }
    }

    // ── Tile build ────────────────────────────────────────────────────────

    /// <summary>
    /// Streams all organizer documents in parallel. Used by the tile build job.
    /// </summary>
    public async IAsyncEnumerable<RaceOrganizerDocument> StreamAllAsync(
        int maxConcurrency = 32,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var doc in StreamOrganizerBlobsAsync(
            maxConcurrency,
            static content => Deserialize(content),
            cancellationToken))
        {
            yield return doc;
        }
    }

    public async IAsyncEnumerable<OrganizerBlobIdentity> StreamIdentitiesAsync(
        int maxConcurrency = 32,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var input in StreamOrganizerBlobsAsync(
            maxConcurrency,
            static content => DeserializeIdentity(content),
            cancellationToken))
        {
            yield return input;
        }
    }

    public async IAsyncEnumerable<OrganizerBlobMetadataDocument> StreamMetadataWithoutGeometriesAsync(
        int maxConcurrency = 32,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var input in StreamOrganizerBlobsAsync(
            maxConcurrency,
            static content => DeserializeMetadataWithoutGeometries(content),
            cancellationToken))
        {
            yield return input;
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    private async Task ModifyAsync(
        string organizerKey,
        Action<RaceOrganizerDocument> modify,
        string? createUrl = null,
        Dictionary<string, string>? metadata = null,
        CancellationToken cancellationToken = default)
    {
        organizerKey = await ResolveOrganizerKeyAsync(organizerKey, cancellationToken);
        var blob = container.GetBlobClient(BlobKey(organizerKey));

        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            RaceOrganizerDocument doc;
            ETag etag;
            bool isNew;

            try
            {
                var download = await blob.DownloadContentAsync(cancellationToken);
                doc = Deserialize(download.Value.Content) ?? new RaceOrganizerDocument { Id = organizerKey, Url = createUrl ?? string.Empty };
                etag = download.Value.Details.ETag;
                isNew = false;
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                doc = new RaceOrganizerDocument { Id = organizerKey, Url = createUrl ?? string.Empty };
                etag = ETag.All; // sentinel — will use IfNoneMatch on upload
                isNew = true;
            }

            modify(doc);

            var uploadOptions = new BlobUploadOptions
            {
                Conditions = isNew
                    ? new BlobRequestConditions { IfNoneMatch = ETag.All }
                    : new BlobRequestConditions { IfMatch = etag },
                Metadata = metadata,
            };

            try
            {
                await blob.UploadAsync(Serialize(doc), uploadOptions, cancellationToken);
                return; // success
            }
            catch (RequestFailedException ex) when (ex.Status is 409 or 412)
            {
                // Concurrent writer — back off and retry.
                if (attempt < MaxRetries - 1)
                    await Task.Delay(TimeSpan.FromMilliseconds(100 * (attempt + 1)), cancellationToken);
            }
        }

        _logger.LogError("Could not update organizer blob '{Key}' after {Retries} retries (concurrent modification)", organizerKey, MaxRetries);
        throw new InvalidOperationException($"Could not update organizer blob '{organizerKey}' after {MaxRetries} retries.");
    }

    private static string BlobKey(string organizerKey) => $"{organizerKey}{BlobSuffix}";

    private static string RedirectBlobKey(string organizerKey) => $"{organizerKey}{RedirectSuffix}";

    private static string AssembledRacePrefix(string organizerKey) => $"{organizerKey}{AssembledRaceFolder}";

    private static string AssembledRaceBlobKey(string organizerKey, string logicalId)
        => $"{organizerKey}{AssembledRaceFolder}{logicalId}.json";

    private static string OrganizerKeyFromBlobName(string blobName)
        => blobName[..^BlobSuffix.Length];

    private static string OrganizerKeyFromRedirectBlobName(string blobName)
        => blobName[..^RedirectSuffix.Length];

    private static BinaryData Serialize(RaceOrganizerDocument doc)
        => BinaryData.FromBytes(JsonSerializer.SerializeToUtf8Bytes(doc, JsonOptions));

    private static RaceOrganizerDocument? Deserialize(BinaryData content)
        => content.ToObjectFromJson<RaceOrganizerDocument>(JsonOptions);

    private static OrganizerBlobIdentity? DeserializeIdentity(BinaryData content)
    {
        using var document = JsonDocument.Parse(content.ToMemory());
        var root = document.RootElement;

        if (!root.TryGetProperty("id", out var idElement) || idElement.ValueKind != JsonValueKind.String)
            return null;

        var id = idElement.GetString();
        if (string.IsNullOrWhiteSpace(id))
            return null;

        if (!root.TryGetProperty("url", out var urlElement) || urlElement.ValueKind != JsonValueKind.String)
            return null;

        var url = urlElement.GetString();
        if (string.IsNullOrWhiteSpace(url))
            return null;

        return new OrganizerBlobIdentity { Id = id, Url = url };
    }

    private static OrganizerBlobMetadataDocument? DeserializeMetadataWithoutGeometries(BinaryData content)
    {
        using var document = JsonDocument.Parse(content.ToMemory());
        var root = document.RootElement;

        if (!root.TryGetProperty("id", out var idElement) || idElement.ValueKind != JsonValueKind.String)
            return null;

        var id = idElement.GetString();
        if (string.IsNullOrWhiteSpace(id))
            return null;

        if (!root.TryGetProperty("url", out var urlElement) || urlElement.ValueKind != JsonValueKind.String)
            return null;

        var url = urlElement.GetString();
        if (string.IsNullOrWhiteSpace(url))
            return null;

        var discovery = root.TryGetProperty("discovery", out var discoveryElement) && discoveryElement.ValueKind == JsonValueKind.Object
            ? discoveryElement.Deserialize<Dictionary<string, List<SourceDiscovery>>>(JsonOptions)
            : null;

        var scrapers = root.TryGetProperty("scrapers", out var scrapersElement) && scrapersElement.ValueKind == JsonValueKind.Object
            ? scrapersElement.Deserialize<Dictionary<string, OrganizerBlobScraperMetadata>>(JsonOptions)
            : null;

        var scraperHashes = root.TryGetProperty("scraperHashes", out var scraperHashesElement) && scraperHashesElement.ValueKind == JsonValueKind.Object
            ? scraperHashesElement.Deserialize<Dictionary<string, ScraperOutputHashes>>(JsonOptions)
            : null;

        var assemblyHashes = root.TryGetProperty("assemblyHashes", out var assemblyHashesElement) && assemblyHashesElement.ValueKind == JsonValueKind.Object
            ? assemblyHashesElement.Deserialize<Dictionary<string, RaceSlotHashes>>(JsonOptions)
            : null;

        var lastScrapedUtc = root.TryGetProperty("lastScrapedUtc", out var lastScrapedUtcElement) && lastScrapedUtcElement.ValueKind == JsonValueKind.String
            ? lastScrapedUtcElement.GetString()
            : null;

        var lastAssembledUtc = root.TryGetProperty("lastAssembledUtc", out var lastAssembledUtcElement) && lastAssembledUtcElement.ValueKind == JsonValueKind.String
            ? lastAssembledUtcElement.GetString()
            : null;

        var lastMaxSlotIndex = root.TryGetProperty("lastMaxSlotIndex", out var lastMaxSlotIndexElement) && lastMaxSlotIndexElement.ValueKind == JsonValueKind.Number
            ? lastMaxSlotIndexElement.GetInt32()
            : (int?)null;

        return new OrganizerBlobMetadataDocument
        {
            Id = id,
            Url = url,
            Discovery = discovery,
            Scrapers = scrapers,
            LastScrapedUtc = lastScrapedUtc,
            ScraperHashes = scraperHashes,
            LastAssembledUtc = lastAssembledUtc,
            LastMaxSlotIndex = lastMaxSlotIndex,
            AssemblyHashes = assemblyHashes,
        };
    }

    private async IAsyncEnumerable<T> StreamOrganizerBlobsAsync<T>(
        int maxConcurrency,
        Func<BinaryData, T?> deserialize,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where T : class
    {
        var blobNames = new List<string>();
        var redirectedKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await foreach (var item in container.GetBlobsAsync(cancellationToken: cancellationToken))
        {
            if (item.Name.EndsWith(RedirectSuffix, StringComparison.Ordinal))
            {
                redirectedKeys.Add(OrganizerKeyFromRedirectBlobName(item.Name));
                continue;
            }

            if (item.Name.EndsWith(BlobSuffix, StringComparison.Ordinal))
                blobNames.Add(item.Name);
        }

        blobNames.RemoveAll(name => redirectedKeys.Contains(OrganizerKeyFromBlobName(name)));

        for (int i = 0; i < blobNames.Count; i += maxConcurrency)
        {
            var batch = blobNames.Skip(i).Take(maxConcurrency).ToList();
            var tasks = batch.Select(async name =>
            {
                var blob = container.GetBlobClient(name);
                try
                {
                    var result = await blob.DownloadContentAsync(cancellationToken);
                    return deserialize(result.Value.Content);
                }
                catch (RequestFailedException ex) when (ex.Status == 404)
                {
                    _logger.LogDebug("Skipping organizer blob {BlobName} because it was deleted after listing.", name);
                    return null;
                }
            }).ToList();

            var docs = await Task.WhenAll(tasks);
            foreach (var doc in docs)
            {
                if (doc is not null)
                    yield return doc;
            }
        }
    }

    private async Task<string?> GetRedirectTargetAsync(string organizerKey, CancellationToken cancellationToken)
    {
        var blob = container.GetBlobClient(RedirectBlobKey(organizerKey));
        try
        {
            var result = await blob.DownloadContentAsync(cancellationToken);
            var target = result.Value.Content.ToString().Trim();
            return string.IsNullOrWhiteSpace(target) ? null : target;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    private async Task DeletePrefixAsync(string prefix, CancellationToken cancellationToken)
    {
        await foreach (var item in container.GetBlobsAsync(prefix: prefix, cancellationToken: cancellationToken))
            await container.DeleteBlobIfExistsAsync(item.Name, cancellationToken: cancellationToken);
    }
}
