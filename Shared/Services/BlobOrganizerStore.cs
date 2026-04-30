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
    private const string AssembledRaceFolder = "/races/";
    private const string MetaLastScrapedUtc = "lastscrapedutc"; // blob metadata keys are lowercase
    private const string MetaTransparencyHash = "transparencyhash";
    private const int MaxRetries = 5;

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
        var blob = container.GetBlobClient(BlobKey(doc.Id));
        await blob.UploadAsync(Serialize(doc), overwrite: true, cancellationToken);
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
        var ids = new List<string>();
        var cutoffStr = cutoffUtc.ToString("o");

        await foreach (var item in container.GetBlobsAsync(BlobTraits.Metadata, cancellationToken: cancellationToken))
        {
            if (!item.Name.EndsWith(BlobSuffix, StringComparison.Ordinal))
                continue;

            item.Metadata.TryGetValue(MetaLastScrapedUtc, out var lastScraped);

            // Include if never scraped or scraped before cutoff.
            if (string.IsNullOrEmpty(lastScraped) || string.Compare(lastScraped, cutoffStr, StringComparison.Ordinal) < 0)
            {
                ids.Add(OrganizerKeyFromBlobName(item.Name));
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
        // Collect all blob names first, then download in parallel batches.
        var blobNames = new List<string>();
        await foreach (var item in container.GetBlobsAsync(cancellationToken: cancellationToken))
        {
            if (item.Name.EndsWith(BlobSuffix, StringComparison.Ordinal))
                blobNames.Add(item.Name);
        }

        // Process in batches.
        for (int i = 0; i < blobNames.Count; i += maxConcurrency)
        {
            var batch = blobNames.Skip(i).Take(maxConcurrency).ToList();
            var tasks = batch.Select(async name =>
            {
                var blob = container.GetBlobClient(name);
                var result = await blob.DownloadContentAsync(cancellationToken);
                return Deserialize(result.Value.Content);
            }).ToList();

            var docs = await Task.WhenAll(tasks);
            foreach (var doc in docs)
            {
                if (doc is not null)
                    yield return doc;
            }
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

    private static string AssembledRacePrefix(string organizerKey) => $"{organizerKey}{AssembledRaceFolder}";

    private static string AssembledRaceBlobKey(string organizerKey, string logicalId)
        => $"{organizerKey}{AssembledRaceFolder}{logicalId}.json";

    private static string OrganizerKeyFromBlobName(string blobName)
        => blobName[..^BlobSuffix.Length];

    private static BinaryData Serialize(RaceOrganizerDocument doc)
        => BinaryData.FromBytes(JsonSerializer.SerializeToUtf8Bytes(doc, JsonOptions));

    private static RaceOrganizerDocument? Deserialize(BinaryData content)
        => JsonSerializer.Deserialize<RaceOrganizerDocument>(content.ToString(), JsonOptions);
}
