using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Stigar.Core.Models;

namespace Stigar.Core.Services;

/// <summary>
/// Self-hosted organizer store backed by local disk. Layout:
/// <c>{root}/{organizerKey}/organizer.json</c>,
/// <c>{root}/{organizerKey}/routes/*.gpx</c>,
/// <c>{root}/{organizerKey}/races/*.json</c> (transparency markers) and
/// <c>{root}/{organizerKey}/redirect.txt</c>.
/// Every write is atomic: it is staged at <c>path + ".tmp"</c> and then moved into place.
/// There is no blob ETag concurrency on disk, so read-modify-write is done directly.
/// </summary>
public sealed class FileSystemOrganizerStore(string root, ILoggerFactory loggerFactory) : IOrganizerStore
{
    private const string OrganizerFileName = "organizer.json";
    private const string RedirectFileName = "redirect.txt";
    private const string RacesFolderName = "races";
    private const int MaxRedirectDepth = 8;

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

    private static readonly UTF8Encoding Utf8NoBom = new(false);

    private readonly string _root = NormalizeRoot(root);
    private readonly ILogger _logger = loggerFactory.CreateLogger<FileSystemOrganizerStore>();
    private OrganizerReadMetricsSnapshot _lastOrganizerReadMetrics;

    // ── Read ─────────────────────────────────────────────────────────────

    public async Task<RaceOrganizerDocument?> GetByIdAsync(
        string organizerKey,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var resolved = await ResolveOrganizerKeyAsync(organizerKey, cancellationToken);

            // Resolve the key against the directories that actually exist under
            // the trusted root. The caller's value is only used for comparison;
            // the file path comes from the enumeration result, never from the
            // user-provided key.
            var directory = FindOrganizerDirectory(resolved);
            if (directory is null)
                return null;

            var path = Path.Combine(directory, OrganizerFileName);
            if (!File.Exists(path))
                return null;

            return Deserialize<RaceOrganizerDocument>(await File.ReadAllBytesAsync(path, cancellationToken));
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    /// <summary>
    /// Finds an existing organizer directory by key. The returned path originates
    /// from <see cref="Directory.EnumerateDirectories(string)"/> under the store
    /// root, so no caller-controlled string is used to build a file path.
    /// </summary>
    private string? FindOrganizerDirectory(string organizerKey)
    {
        if (!Directory.Exists(_root))
            return null;

        foreach (var directory in Directory.EnumerateDirectories(_root))
        {
            if (string.Equals(Path.GetFileName(directory), organizerKey, StringComparison.OrdinalIgnoreCase))
                return directory;
        }

        return null;
    }

    // ── Write ────────────────────────────────────────────────────────────

    public async Task WriteAsync(RaceOrganizerDocument doc, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(doc);
        doc.Id = await ResolveOrganizerKeyAsync(doc.Id, cancellationToken);
        WriteOrganizerJson(doc.Id, doc);
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

            ValidateOrganizerKey(current);
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

        ValidateOrganizerKey(source);
        ValidateOrganizerKey(target);

        var sourceDirectory = OrganizerDirectory(source);
        if (Directory.Exists(sourceDirectory))
            Directory.Delete(sourceDirectory, recursive: true);

        var redirectPath = RedirectPath(source);
        WriteFileAtomically(redirectPath, Utf8NoBom.GetBytes(target + "\n"));
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
            cancellationToken: cancellationToken);
    }

    // ── Query ─────────────────────────────────────────────────────────────

    public async Task<List<string>> GetIdsDueForAutomaticScrapeAsync(
        DateTime cutoffUtc,
        CancellationToken cancellationToken = default)
    {
        var ids = new List<string>();
        if (!Directory.Exists(_root))
            return ids;

        var cutoffStr = cutoffUtc.ToString("o");
        foreach (var directory in Directory.EnumerateDirectories(_root))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var organizerKey = Path.GetFileName(directory);
            if (string.IsNullOrWhiteSpace(organizerKey) || File.Exists(RedirectPath(organizerKey)))
                continue;

            var path = OrganizerJsonPath(organizerKey);
            if (!File.Exists(path))
                continue;

            var doc = Deserialize<RaceOrganizerDocument>(await File.ReadAllBytesAsync(path, cancellationToken));
            var lastScraped = doc?.LastScrapedUtc;

            // Include if never scraped or scraped before cutoff.
            if (string.IsNullOrEmpty(lastScraped) || string.Compare(lastScraped, cutoffStr, StringComparison.Ordinal) < 0)
                ids.Add(organizerKey);
        }

        return ids;
    }

    public Task<(int OrganizerCount, int AssembledRaceCount)> GetCountsAsync(CancellationToken cancellationToken = default)
    {
        var organizerCount = 0;
        var assembledRaceCount = 0;
        if (!Directory.Exists(_root))
            return Task.FromResult((organizerCount, assembledRaceCount));

        foreach (var path in Directory.EnumerateFiles(_root, "*.json", SearchOption.AllDirectories))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (string.Equals(Path.GetFileName(path), OrganizerFileName, StringComparison.OrdinalIgnoreCase))
            {
                organizerCount++;
                continue;
            }

            var parentName = Path.GetFileName(Path.GetDirectoryName(path));
            if (string.Equals(parentName, RacesFolderName, StringComparison.OrdinalIgnoreCase))
                assembledRaceCount++;
        }

        return Task.FromResult((organizerCount, assembledRaceCount));
    }

    // ── GPX routes ────────────────────────────────────────────────────────

    public async Task UploadRouteGpxAsync(
        string blobPath,
        byte[] gpxBytes,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(gpxBytes);
        var path = ResolveRelativePath(blobPath, nameof(blobPath));
        await WriteFileAtomicallyAsync(path, gpxBytes, cancellationToken);
    }

    // ── Transparency ──────────────────────────────────────────────────────

    public async Task<TransparencyWriteStats> WriteAssembledRacesAsync(
        string organizerKey,
        IReadOnlyList<StoredFeature> races,
        CancellationToken cancellationToken = default)
    {
        var resolved = await ResolveOrganizerKeyAsync(organizerKey, cancellationToken);
        var racesDirectory = RacesDirectory(resolved);
        var existingHashes = new Dictionary<string, string>(StringComparer.Ordinal);
        if (Directory.Exists(racesDirectory))
        {
            foreach (var file in Directory.EnumerateFiles(racesDirectory, "*.json", SearchOption.TopDirectoryOnly))
            {
                var bytes = await File.ReadAllBytesAsync(file, cancellationToken);
                existingHashes[Path.GetFileNameWithoutExtension(file)] = ComputeHash(bytes);
            }
        }

        var raceUploads = 0;
        var raceUnchanged = 0;
        var retainedIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var race in races)
        {
            var logicalId = !string.IsNullOrWhiteSpace(race.FeatureId)
                ? race.FeatureId
                : race.LogicalId;
            ValidateSingleSegment(logicalId, nameof(race));
            retainedIds.Add(logicalId);

            var marker = RaceAssembler.CreateTransparencyMarker(race);
            var payload = JsonSerializer.SerializeToUtf8Bytes(marker, TransparencyJsonOptions);
            var hash = ComputeHash(payload);

            if (existingHashes.TryGetValue(logicalId, out var existingHash)
                && string.Equals(existingHash, hash, StringComparison.Ordinal))
            {
                raceUnchanged++;
                continue;
            }

            await WriteFileAtomicallyAsync(RacePath(resolved, logicalId), payload, cancellationToken);
            raceUploads++;
        }

        var raceDeletes = 0;
        if (Directory.Exists(racesDirectory))
        {
            foreach (var file in Directory.EnumerateFiles(racesDirectory, "*.json", SearchOption.TopDirectoryOnly))
            {
                if (retainedIds.Contains(Path.GetFileNameWithoutExtension(file)))
                    continue;

                File.Delete(file);
                raceDeletes++;
            }
        }

        return new TransparencyWriteStats(1, raceUploads, raceDeletes, raceUnchanged);
    }

    // ── Tile build streams ────────────────────────────────────────────────

    public async IAsyncEnumerable<RaceOrganizerDocument> StreamAllAsync(
        int maxConcurrency = 32,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in StreamAllWithSourceAsync(maxConcurrency, cancellationToken))
            yield return item.Document;
    }

    public async IAsyncEnumerable<OrganizerDocumentStreamItem> StreamAllWithSourceAsync(
        int maxConcurrency = 32,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var streamedDocuments = 0;
        long totalBytes = 0;
        await foreach (var (_, bytes) in EnumerateOrganizerFilesAsync(cancellationToken))
        {
            var doc = Deserialize<RaceOrganizerDocument>(bytes);
            if (doc is null)
                continue;

            streamedDocuments++;
            totalBytes += bytes.Length;
            yield return new OrganizerDocumentStreamItem(doc, OrganizerDocumentSource.FileSystem);
        }

        LogReadSummary(streamedDocuments, totalBytes);
    }

    public async IAsyncEnumerable<OrganizerBlobIdentity> StreamIdentitiesAsync(
        int maxConcurrency = 32,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var (_, bytes) in EnumerateOrganizerFilesAsync(cancellationToken))
        {
            var identity = DeserializeIdentity(bytes);
            if (identity is not null)
                yield return identity;
        }
    }

    public async IAsyncEnumerable<OrganizerBlobMetadataDocument> StreamMetadataWithoutGeometriesAsync(
        int maxConcurrency = 32,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var (_, bytes) in EnumerateOrganizerFilesAsync(cancellationToken))
        {
            var metadata = DeserializeMetadataWithoutGeometries(bytes);
            if (metadata is not null)
                yield return metadata;
        }
    }

    public async IAsyncEnumerable<OrganizerRedirectEntry> StreamRedirectsAsync(
        int maxConcurrency = 32,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(_root))
            yield break;

        foreach (var directory in Directory.EnumerateDirectories(_root))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var organizerKey = Path.GetFileName(directory);
            if (string.IsNullOrWhiteSpace(organizerKey))
                continue;

            var redirectPath = Path.Combine(directory, RedirectFileName);
            if (!File.Exists(redirectPath))
                continue;

            var target = (await File.ReadAllTextAsync(redirectPath, Utf8NoBom, cancellationToken)).Trim();
            if (string.IsNullOrWhiteSpace(target))
                continue;

            yield return new OrganizerRedirectEntry(organizerKey, target);
        }
    }

    public async IAsyncEnumerable<(string OrganizerKey, StoredFeature Race)> StreamAssembledRacesAsync(
        string? organizerKey = null,
        int maxConcurrency = 32,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var requestedKey = string.IsNullOrWhiteSpace(organizerKey)
            ? null
            : await ResolveOrganizerKeyAsync(organizerKey, cancellationToken);

        if (!Directory.Exists(_root))
            yield break;

        var organizerDirectories = requestedKey is null
            ? Directory.EnumerateDirectories(_root).ToList()
            : [OrganizerDirectory(requestedKey)];

        foreach (var directory in organizerDirectories)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var key = Path.GetFileName(directory);
            if (string.IsNullOrWhiteSpace(key))
                continue;

            var racesDirectory = Path.Combine(directory, RacesFolderName);
            if (!Directory.Exists(racesDirectory))
                continue;

            foreach (var file in Directory.EnumerateFiles(racesDirectory, "*.json", SearchOption.TopDirectoryOnly))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var race = Deserialize<StoredFeature>(await File.ReadAllBytesAsync(file, cancellationToken));
                if (race is not null)
                    yield return (key, race);
            }
        }
    }

    public async Task<StoredFeature?> GetAssembledRaceAsync(
        string organizerKey,
        string featureId,
        CancellationToken cancellationToken = default)
    {
        var resolved = await ResolveOrganizerKeyAsync(organizerKey, cancellationToken);
        var path = RacePath(resolved, featureId);
        if (!File.Exists(path))
            return null;

        return Deserialize<StoredFeature>(await File.ReadAllBytesAsync(path, cancellationToken));
    }

    public OrganizerReadMetricsSnapshot GetLastOrganizerReadMetricsSnapshot()
        => _lastOrganizerReadMetrics;

    // ── Internal helpers ──────────────────────────────────────────────────

    private async Task ModifyAsync(
        string organizerKey,
        Action<RaceOrganizerDocument> modify,
        string? createUrl = null,
        CancellationToken cancellationToken = default)
    {
        var resolved = await ResolveOrganizerKeyAsync(organizerKey, cancellationToken);
        var path = OrganizerJsonPath(resolved);

        var doc = File.Exists(path)
            ? Deserialize<RaceOrganizerDocument>(await File.ReadAllBytesAsync(path, cancellationToken))
            : null;
        doc ??= new RaceOrganizerDocument { Id = resolved, Url = createUrl ?? string.Empty };

        modify(doc);
        WriteOrganizerJson(resolved, doc);
    }

    private async IAsyncEnumerable<(string OrganizerKey, byte[] Bytes)> EnumerateOrganizerFilesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (!Directory.Exists(_root))
            yield break;

        foreach (var directory in Directory.EnumerateDirectories(_root))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var organizerKey = Path.GetFileName(directory);
            if (string.IsNullOrWhiteSpace(organizerKey) || File.Exists(Path.Combine(directory, RedirectFileName)))
                continue;

            var path = Path.Combine(directory, OrganizerFileName);
            if (!File.Exists(path))
                continue;

            byte[] bytes;
            try
            {
                bytes = await File.ReadAllBytesAsync(path, cancellationToken);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                _logger.LogDebug(ex, "Skipping unreadable organizer file {Path}.", path);
                continue;
            }

            yield return (organizerKey, bytes);
        }
    }

    private async Task<string?> GetRedirectTargetAsync(string organizerKey, CancellationToken cancellationToken)
    {
        var path = RedirectPath(organizerKey);
        if (!File.Exists(path))
            return null;

        var target = (await File.ReadAllTextAsync(path, Utf8NoBom, cancellationToken)).Trim();
        return string.IsNullOrWhiteSpace(target) ? null : target;
    }

    private void WriteOrganizerJson(string organizerKey, RaceOrganizerDocument doc)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(doc, JsonOptions);
        WriteFileAtomically(OrganizerJsonPath(organizerKey), payload);
    }

    private string OrganizerDirectory(string organizerKey)
    {
        ValidateOrganizerKey(organizerKey);

        // Sanitize each segment with Path.GetFileName (a recognized path
        // sanitizer) before combining, then confirm the result stays under the
        // configured root. This keeps multi-segment keys intact while making
        // traversal impossible even if validation ever changes.
        var segments = organizerKey
            .Split(['/', '\\', Path.DirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries)
            .Select(segment => Path.GetFileName(segment))
            .Where(segment => segment.Length > 0 && segment != "." && segment != "..")
            .ToArray();
        var directory = Path.GetFullPath(Path.Combine([_root, .. segments]));
        EnsureUnderRoot(directory, organizerKey);
        return directory;
    }

    private void EnsureUnderRoot(string fullPath, string originalValue)
    {
        var rootPrefix = _root.EndsWith(Path.DirectorySeparatorChar)
            ? _root
            : _root + Path.DirectorySeparatorChar;
        if (!fullPath.StartsWith(rootPrefix, StringComparison.Ordinal)
            && !string.Equals(fullPath, _root, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Path for '{originalValue}' escapes the organizer store root.",
                nameof(originalValue));
        }
    }

    private string OrganizerJsonPath(string organizerKey)
        => Path.Combine(OrganizerDirectory(organizerKey), OrganizerFileName);

    private string RedirectPath(string organizerKey)
        => Path.Combine(OrganizerDirectory(organizerKey), RedirectFileName);

    private string RacesDirectory(string organizerKey)
        => Path.Combine(OrganizerDirectory(organizerKey), RacesFolderName);

    private string RacePath(string organizerKey, string featureId)
    {
        ValidateSingleSegment(featureId, nameof(featureId));
        return Path.Combine(RacesDirectory(organizerKey), featureId + ".json");
    }

    private string ResolveRelativePath(string relativePath, string paramName)
    {
        if (string.IsNullOrWhiteSpace(relativePath))
            throw new ArgumentException("A root-relative path is required.", paramName);

        if (Path.IsPathRooted(relativePath))
            throw new ArgumentException($"Path '{relativePath}' must be root-relative.", paramName);

        var segments = relativePath.Split('/', '\\', Path.DirectorySeparatorChar);
        if (segments.Any(segment => segment == ".."))
            throw new ArgumentException($"Path '{relativePath}' must not contain '..' segments.", paramName);

        var normalized = relativePath
            .Replace('/', Path.DirectorySeparatorChar)
            .Replace('\\', Path.DirectorySeparatorChar);
        var fullPath = Path.GetFullPath(Path.Combine(_root, normalized));
        var rootPrefix = _root.EndsWith(Path.DirectorySeparatorChar)
            ? _root
            : _root + Path.DirectorySeparatorChar;
        if (!fullPath.StartsWith(rootPrefix, StringComparison.Ordinal)
            && !string.Equals(fullPath, _root, StringComparison.Ordinal))
        {
            throw new ArgumentException($"Path '{relativePath}' escapes the organizer store root.", paramName);
        }

        return fullPath;
    }

    private static void ValidateOrganizerKey(string organizerKey, string paramName = "organizerKey")
    {
        if (string.IsNullOrWhiteSpace(organizerKey))
            throw new ArgumentException("Organizer key is required.", paramName);

        if (Path.IsPathRooted(organizerKey))
            throw new ArgumentException($"Organizer key '{organizerKey}' must be relative.", paramName);

        var segments = organizerKey.Split('/', '\\', Path.DirectorySeparatorChar);
        if (segments.Any(segment => segment == ".."))
            throw new ArgumentException($"Organizer key '{organizerKey}' must not contain '..' segments.", paramName);

        if (Path.DirectorySeparatorChar != '/' && organizerKey.Contains('/')
            || Path.DirectorySeparatorChar != '\\' && organizerKey.Contains('\\'))
        {
            throw new ArgumentException(
                $"Organizer key '{organizerKey}' must not contain directory separators other than the platform separator.",
                paramName);
        }
    }

    private static void ValidateSingleSegment(string value, string paramName)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("A file name is required.", paramName);

        if (value.Contains('/') || value.Contains('\\') || value == ".." || value == ".")
            throw new ArgumentException($"Value '{value}' must be a single path segment.", paramName);
    }

    private static string NormalizeRoot(string root)
    {
        if (string.IsNullOrWhiteSpace(root))
            throw new ArgumentException("An organizer store root directory is required.", nameof(root));

        return Path.GetFullPath(root.Trim());
    }

    private static T? Deserialize<T>(byte[] bytes)
    {
        try
        {
            return JsonSerializer.Deserialize<T>(bytes, JsonOptions);
        }
        catch (JsonException)
        {
            return default;
        }
    }

    private static OrganizerBlobIdentity? DeserializeIdentity(byte[] bytes)
    {
        using var document = JsonDocument.Parse(new ReadOnlyMemory<byte>(bytes));
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

    private static OrganizerBlobMetadataDocument? DeserializeMetadataWithoutGeometries(byte[] bytes)
    {
        using var document = JsonDocument.Parse(new ReadOnlyMemory<byte>(bytes));
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

    private static string ComputeHash(byte[] payload)
        => Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(payload)).ToLowerInvariant();

    private void LogReadSummary(int streamedDocuments, long totalBytes)
    {
        _lastOrganizerReadMetrics = new OrganizerReadMetricsSnapshot(streamedDocuments, 0, 0, 0, 0);
        _logger.LogInformation(
            "Organizer reads: {Count} docs from file system ({Bytes}).",
            streamedDocuments,
            OrganizerStoreFormatting.FormatByteCountForLogging(totalBytes));
    }

    private static void WriteFileAtomically(string path, byte[] bytes)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        var tempPath = path + ".tmp";
        File.WriteAllBytes(tempPath, bytes);
        File.Move(tempPath, path, overwrite: true);
    }

    private static async Task WriteFileAtomicallyAsync(string path, byte[] bytes, CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        var tempPath = path + ".tmp";
        await File.WriteAllBytesAsync(tempPath, bytes, cancellationToken);
        File.Move(tempPath, path, overwrite: true);
    }
}
