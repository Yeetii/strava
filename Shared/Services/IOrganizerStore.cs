using Shared.Models;

namespace Shared.Services;

/// <summary>
/// Storage abstraction for race organizer documents, route GPX files, redirects and the
/// per-organizer transparency markers. Implemented by both the blob-backed store and the
/// self-hosted filesystem store.
/// </summary>
public interface IOrganizerStore
{
    Task<RaceOrganizerDocument?> GetByIdAsync(string organizerKey, CancellationToken cancellationToken = default);

    Task WriteAsync(RaceOrganizerDocument doc, CancellationToken cancellationToken = default);

    Task<string> ResolveOrganizerKeyAsync(string organizerKey, CancellationToken cancellationToken = default);

    Task SetRedirectAsync(string sourceOrganizerKey, string targetOrganizerKey, CancellationToken cancellationToken = default);

    Task WriteDiscoveryAsync(string organizerKey, string canonicalUrl, string source, List<SourceDiscovery> discoveries, CancellationToken cancellationToken = default);

    Task WriteDiscoveriesAsync(string source, IReadOnlyList<(string OrganizerKey, string CanonicalUrl, List<SourceDiscovery> Discoveries)> items, CancellationToken cancellationToken = default);

    Task WriteScraperOutputAsync(string organizerKey, string scraperKey, ScraperOutput output, CancellationToken cancellationToken = default);

    Task PatchScraperPropertiesAsync(string organizerKey, string scraperKey, ScraperOutput output, CancellationToken cancellationToken = default);

    Task PatchLastScrapedAsync(string organizerKey, string lastScrapedUtc, Dictionary<string, ScraperOutputHashes>? scraperHashes, CancellationToken cancellationToken = default);

    Task<List<string>> GetIdsDueForAutomaticScrapeAsync(DateTime cutoffUtc, CancellationToken cancellationToken = default);

    Task<(int OrganizerCount, int AssembledRaceCount)> GetCountsAsync(CancellationToken cancellationToken = default);

    Task UploadRouteGpxAsync(string blobPath, byte[] gpxBytes, CancellationToken cancellationToken = default);

    Task<TransparencyWriteStats> WriteAssembledRacesAsync(string organizerKey, IReadOnlyList<StoredFeature> races, CancellationToken cancellationToken = default);

    IAsyncEnumerable<RaceOrganizerDocument> StreamAllAsync(int maxConcurrency = 32, CancellationToken cancellationToken = default);

    IAsyncEnumerable<OrganizerDocumentStreamItem> StreamAllWithSourceAsync(int maxConcurrency = 32, CancellationToken cancellationToken = default);

    IAsyncEnumerable<OrganizerBlobIdentity> StreamIdentitiesAsync(int maxConcurrency = 32, CancellationToken cancellationToken = default);

    IAsyncEnumerable<OrganizerBlobMetadataDocument> StreamMetadataWithoutGeometriesAsync(int maxConcurrency = 32, CancellationToken cancellationToken = default);

    IAsyncEnumerable<OrganizerRedirectEntry> StreamRedirectsAsync(int maxConcurrency = 32, CancellationToken cancellationToken = default);

    IAsyncEnumerable<(string OrganizerKey, StoredFeature Race)> StreamAssembledRacesAsync(string? organizerKey = null, int maxConcurrency = 32, CancellationToken cancellationToken = default);

    Task<StoredFeature?> GetAssembledRaceAsync(string organizerKey, string featureId, CancellationToken cancellationToken = default);

    OrganizerReadMetricsSnapshot GetLastOrganizerReadMetricsSnapshot();
}

/// <summary>Where an organizer document was read from during a stream.</summary>
public enum OrganizerDocumentSource
{
    Cache,
    Blob,
    FileSystem,
}

/// <summary>Counters returned by a transparency write.</summary>
public readonly record struct TransparencyWriteStats(int OrganizerWrites, int RaceUploads, int RaceDeletes, int RaceUnchanged);

/// <summary>An organizer document paired with the source it was read from.</summary>
public sealed record OrganizerDocumentStreamItem(RaceOrganizerDocument Document, OrganizerDocumentSource Source);

/// <summary>Snapshot of the most recent organizer read stream.</summary>
public readonly record struct OrganizerReadMetricsSnapshot(
    int StreamedDocuments,
    int CacheHits,
    long CachedBytes,
    int BlobDownloads,
    long DownloadedBytes);

/// <summary>Shared human-readable byte formatting for organizer read logging.</summary>
public static class OrganizerStoreFormatting
{
    public static string FormatByteCountForLogging(long byteCount)
    {
        string[] suffixes = ["B", "KB", "MB", "GB"];
        double value = byteCount;
        var suffixIndex = 0;
        while (value >= 1024 && suffixIndex < suffixes.Length - 1)
        {
            value /= 1024;
            suffixIndex++;
        }

        var format = suffixIndex == 0 ? "0" : "0.##";
        return string.Format(System.Globalization.CultureInfo.InvariantCulture, $"{{0:{format}}} {{1}}", value, suffixes[suffixIndex]);
    }
}
