using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging;
using Moq;
using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class BlobOrganizerStoreRedirectTests
{
    [Fact]
    public async Task SetRedirectAsync_WipesSourcePrefixAndStoresRedirectMarker()
    {
        var blobs = new InMemoryBlobContainer();
        var store = CreateStore(blobs);

        blobs.SeedBlob("alias.example/organizer.json", BinaryData.FromString("{}"));
        blobs.SeedBlob("alias.example/races/stale.json", BinaryData.FromString("{}"));

        await store.SetRedirectAsync("alias.example", "canonical.example");

        Assert.False(blobs.Contains("alias.example/organizer.json"));
        Assert.False(blobs.Contains("alias.example/races/stale.json"));
        Assert.True(blobs.Contains("alias.example/redirect.txt"));
        Assert.Equal("canonical.example\n", blobs.GetContent("alias.example/redirect.txt"));
    }

    [Fact]
    public async Task WriteDiscoveryAsync_UsesRedirectTargetOrganizer()
    {
        var blobs = new InMemoryBlobContainer();
        var store = CreateStore(blobs);
        await store.SetRedirectAsync("alias.example", "canonical.example");

        var discovery = new SourceDiscovery
        {
            DiscoveredAtUtc = "2026-04-30T12:00:00Z",
            Name = "Redirected race",
            SourceUrls = ["https://alias.example/race"]
        };

        await store.WriteDiscoveryAsync(
            "alias.example",
            "https://alias.example/",
            "manual",
            [discovery]);

        var docByAlias = await store.GetByIdAsync("alias.example");
        var docByTarget = await store.GetByIdAsync("canonical.example");

        Assert.NotNull(docByAlias);
        Assert.NotNull(docByTarget);
        Assert.Equal("canonical.example", docByAlias!.Id);
        Assert.Equal("canonical.example", docByTarget!.Id);
        Assert.Single(docByTarget.Discovery!["manual"]);
        Assert.False(blobs.Contains("alias.example/organizer.json"));
        Assert.True(blobs.Contains("canonical.example/organizer.json"));
    }

    [Fact]
    public async Task StreamAllAsync_AndDueScrape_SkipRedirectedOrganizerIds()
    {
        var blobs = new InMemoryBlobContainer();
        var store = CreateStore(blobs);

        SeedOrganizer(blobs, "canonical.example", "https://canonical.example/", "2026-04-01T00:00:00Z");
        SeedOrganizer(blobs, "alias.example", "https://alias.example/", "2026-04-01T00:00:00Z");
        SeedOrganizer(blobs, "other.example", "https://other.example/", "2026-04-01T00:00:00Z");
        blobs.SeedBlob("alias.example/redirect.txt", BinaryData.FromString("canonical.example\n"));

        var idsDueForScrape = await store.GetIdsDueForAutomaticScrapeAsync(new DateTime(2026, 4, 10, 0, 0, 0, DateTimeKind.Utc));
        var streamedIds = new List<string>();
        await foreach (var doc in store.StreamAllAsync())
            streamedIds.Add(doc.Id);

        Assert.DoesNotContain("alias.example", idsDueForScrape);
        Assert.Contains("canonical.example", idsDueForScrape);
        Assert.Contains("other.example", idsDueForScrape);

        Assert.DoesNotContain("alias.example", streamedIds);
        Assert.Contains("canonical.example", streamedIds);
        Assert.Contains("other.example", streamedIds);
    }

    [Fact]
    public async Task StreamRedirectsAsync_ReturnsRedirectEntries()
    {
        var blobs = new InMemoryBlobContainer();
        var store = CreateStore(blobs);

        blobs.SeedBlob("alias.example/redirect.txt", BinaryData.FromString("canonical.example\n"));
        blobs.SeedBlob("other.example/redirect.txt", BinaryData.FromString("target.example\n"));

        var redirects = new List<OrganizerRedirectEntry>();
        await foreach (var redirect in store.StreamRedirectsAsync())
            redirects.Add(redirect);

        Assert.Equal(2, redirects.Count);
        Assert.Contains(redirects, redirect => redirect.SourceOrganizerKey == "alias.example" && redirect.TargetOrganizerKey == "canonical.example");
        Assert.Contains(redirects, redirect => redirect.SourceOrganizerKey == "other.example" && redirect.TargetOrganizerKey == "target.example");
    }

    [Fact]
    public async Task StreamAllAsync_SkipsBlobDeletedAfterListing()
    {
        var blobs = new InMemoryBlobContainer();
        var store = CreateStore(blobs);

        SeedOrganizer(blobs, "stable.example", "https://stable.example/", "2026-04-01T00:00:00Z");
        SeedOrganizer(blobs, "deleted.example", "https://deleted.example/", "2026-04-01T00:00:00Z");
        blobs.DeleteOnFirstDownload("deleted.example/organizer.json");

        var streamedIds = new List<string>();
        await foreach (var doc in store.StreamAllAsync())
            streamedIds.Add(doc.Id);

        Assert.Contains("stable.example", streamedIds);
        Assert.DoesNotContain("deleted.example", streamedIds);
    }

    [Fact]
    public async Task StreamAllAsync_UsesHierarchyListingInsteadOfFlatContainerScan()
    {
        var blobs = new InMemoryBlobContainer();
        var store = CreateStore(blobs);

        SeedOrganizer(blobs, "stable.example", "https://stable.example/", "2026-04-01T00:00:00Z");
        blobs.SeedBlob("stable.example/races/slot-1.json", BinaryData.FromString("{}"));

        var streamedIds = new List<string>();
        await foreach (var doc in store.StreamAllAsync())
            streamedIds.Add(doc.Id);

        Assert.Equal(["stable.example"], streamedIds);
        Assert.Equal(2, blobs.HierarchyListCalls);
        Assert.Equal(0, blobs.FlatListCalls);
    }

    [Fact]
    public async Task StreamAllAsync_ReusesLocalCacheWhenOrganizerBlobEtagIsUnchanged()
    {
        var blobs = new InMemoryBlobContainer();
        var cacheRoot = Path.Combine(Path.GetTempPath(), nameof(BlobOrganizerStoreRedirectTests), Guid.NewGuid().ToString("N"));
        var store = CreateStore(blobs, cacheRoot);

        SeedOrganizer(blobs, "stable.example", "https://stable.example/", "2026-04-01T00:00:00Z");

        var firstPassIds = new List<string>();
        await foreach (var doc in store.StreamAllAsync())
            firstPassIds.Add(doc.Id);

        var secondPassIds = new List<string>();
        await foreach (var doc in store.StreamAllAsync())
            secondPassIds.Add(doc.Id);

        Assert.Equal(["stable.example"], firstPassIds);
        Assert.Equal(["stable.example"], secondPassIds);
        Assert.Equal(1, blobs.GetDownloadCount("stable.example/organizer.json"));
    }

    [Fact]
    public async Task StreamAllWithSourceAsync_ReturnsCacheSourceOnSecondPass()
    {
        var blobs = new InMemoryBlobContainer();
        var cacheRoot = Path.Combine(Path.GetTempPath(), nameof(BlobOrganizerStoreRedirectTests), Guid.NewGuid().ToString("N"));
        var store = CreateStore(blobs, cacheRoot);

        SeedOrganizer(blobs, "stable.example", "https://stable.example/", "2026-04-01T00:00:00Z");

        var firstPass = new List<BlobOrganizerStore.OrganizerDocumentStreamItem>();
        await foreach (var item in store.StreamAllWithSourceAsync())
            firstPass.Add(item);

        var secondPass = new List<BlobOrganizerStore.OrganizerDocumentStreamItem>();
        await foreach (var item in store.StreamAllWithSourceAsync())
            secondPass.Add(item);

        Assert.Equal(BlobOrganizerStore.OrganizerDocumentSource.Blob, Assert.Single(firstPass).Source);
        Assert.Equal(BlobOrganizerStore.OrganizerDocumentSource.Cache, Assert.Single(secondPass).Source);
    }

    [Fact]
    public async Task WriteAssembledRacesAsync_ReusesLocalTransparencyManifestOnSecondRun()
    {
        var blobs = new InMemoryBlobContainer();
        var cacheRoot = Path.Combine(Path.GetTempPath(), nameof(BlobOrganizerStoreRedirectTests), Guid.NewGuid().ToString("N"));
        var store = CreateStore(blobs, cacheRoot);
        var races = new List<StoredFeature>
        {
            new()
            {
                Id = "race:stable.example-0",
                FeatureId = "stable.example-0",
                Kind = FeatureKinds.Race,
                X = 1,
                Y = 2,
                Zoom = RaceCollectionClient.DefaultZoom,
                Geometry = new Point(new Position(12, 34)),
                Properties = new Dictionary<string, dynamic> { ["name"] = "Stable race" },
            }
        };

        await store.WriteAssembledRacesAsync("stable.example", races);
        var flatListCallsAfterFirstWrite = blobs.FlatListCalls;

        await store.WriteAssembledRacesAsync("stable.example", races);

        Assert.Equal(flatListCallsAfterFirstWrite, blobs.FlatListCalls);
    }

        [Fact]
        public async Task StreamIdentitiesAsync_ReturnsIdAndUrlOnly()
        {
                var blobs = new InMemoryBlobContainer();
                var store = CreateStore(blobs);

                var payload = """
                        {
                            "id": "identity.example",
                            "url": "https://identity.example/",
                            "discovery": {
                                "manual": [
                                    {
                                        "discoveredAtUtc": "2026-04-30T00:00:00Z",
                                        "sourceUrls": ["https://identity.example/race"]
                                    }
                                ]
                            }
                        }
                        """;

                blobs.SeedBlob("identity.example/organizer.json", BinaryData.FromString(payload));

                var identities = new List<OrganizerBlobIdentity>();
                await foreach (var identity in store.StreamIdentitiesAsync())
                        identities.Add(identity);

                var organizer = Assert.Single(identities);
                Assert.Equal("identity.example", organizer.Id);
                Assert.Equal("https://identity.example/", organizer.Url);
        }

        [Fact]
        public async Task StreamMetadataWithoutGeometriesAsync_ReturnsMetadataWithoutCoordinates()
        {
                var blobs = new InMemoryBlobContainer();
                var store = CreateStore(blobs);

                var payload = """
                        {
                            "id": "runsignup.com~Race~TX~Longview~LongviewTrailRunsSpring",
                            "url": "https://runsignup.com/Race/TX/Longview/LongviewTrailRunsSpring",
                            "discovery": {
                                "manual": [
                                    {
                                        "discoveredAtUtc": "2026-04-30T00:00:00Z",
                                        "sourceUrls": [
                                            "https://longviewtrailruns.com/",
                                            "https://runsignup.com/Race/TX/Longview/LongviewTrailRunsSpring"
                                        ]
                                    }
                                ]
                            },
                            "scrapers": {
                                "bfs": {
                                    "scrapedAtUtc": "2026-04-30T00:00:00Z",
                                    "routes": [
                                        {
                                            "name": "large payload we should ignore",
                                            "coordinates": [[1,2],[3,4]]
                                        }
                                    ]
                                }
                            }
                        }
                        """;

                blobs.SeedBlob("runsignup.com~Race~TX~Longview~LongviewTrailRunsSpring/organizer.json", BinaryData.FromString(payload));

        var metadataDocs = new List<OrganizerBlobMetadataDocument>();
        await foreach (var metadata in store.StreamMetadataWithoutGeometriesAsync())
            metadataDocs.Add(metadata);

        var metadataDoc = Assert.Single(metadataDocs);
        Assert.Equal("runsignup.com~Race~TX~Longview~LongviewTrailRunsSpring", metadataDoc.Id);
        Assert.Equal("https://runsignup.com/Race/TX/Longview/LongviewTrailRunsSpring", metadataDoc.Url);
        Assert.NotNull(metadataDoc.Discovery);
        Assert.NotNull(metadataDoc.Scrapers);

        var route = Assert.Single(metadataDoc.Scrapers!["bfs"].Routes!);
        Assert.Equal("large payload we should ignore", route.Name);
        Assert.Equal("https://longviewtrailruns.com/", Assert.Single(metadataDoc.Discovery!["manual"]).SourceUrls![0]);
        }

    private static BlobOrganizerStore CreateStore(InMemoryBlobContainer blobs, string? cacheRoot = null)
        => new(
            blobs.Client.Object,
            LoggerFactory.Create(builder => { }),
            cacheRoot ?? Path.Combine(Path.GetTempPath(), nameof(BlobOrganizerStoreRedirectTests), Guid.NewGuid().ToString("N")));

    private static void SeedOrganizer(InMemoryBlobContainer blobs, string id, string url, string lastScrapedUtc)
    {
        var doc = new RaceOrganizerDocument
        {
            Id = id,
            Url = url,
            LastScrapedUtc = lastScrapedUtc,
        };

        blobs.SeedBlob(
            $"{id}/organizer.json",
            BinaryData.FromString(System.Text.Json.JsonSerializer.Serialize(doc)),
            new Dictionary<string, string> { ["lastscrapedutc"] = lastScrapedUtc });
    }

    private sealed class InMemoryBlobContainer
    {
        private readonly Dictionary<string, BlobState> _blobs = new(StringComparer.Ordinal);
        private readonly Dictionary<string, Mock<BlobClient>> _clients = new(StringComparer.Ordinal);
        private readonly Dictionary<string, int> _downloadCounts = new(StringComparer.Ordinal);

        public Mock<BlobContainerClient> Client { get; } = new();
        public int FlatListCalls { get; private set; }
        public int HierarchyListCalls { get; private set; }

        public InMemoryBlobContainer()
        {
            Client
                .Setup(container => container.GetBlobClient(It.IsAny<string>()))
                .Returns((string blobName) => GetOrCreateBlobClient(blobName).Object);

            Client
                .Setup(container => container.GetBlobsAsync(
                    It.IsAny<BlobTraits>(),
                    It.IsAny<BlobStates>(),
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>()))
                .Returns((BlobTraits _, BlobStates _, string? prefix, CancellationToken __) =>
                {
                    FlatListCalls++;
                    return AsyncPageable<BlobItem>.FromPages(
                    [
                        Page<BlobItem>.FromValues(
                            _blobs
                                .Where(entry => prefix is null || entry.Key.StartsWith(prefix, StringComparison.Ordinal))
                                .OrderBy(entry => entry.Key, StringComparer.Ordinal)
                                .Select(entry => CreateBlobItem(entry.Key, entry.Value.Metadata))
                                .ToList(),
                            continuationToken: null,
                            Mock.Of<Response>())
                    ]);
                });

            Client
                .Setup(container => container.GetBlobsByHierarchyAsync(
                    It.IsAny<BlobTraits>(),
                    It.IsAny<BlobStates>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>()))
                .Returns((BlobTraits _, BlobStates _, string delimiter, string? prefix, CancellationToken __) =>
                {
                    HierarchyListCalls++;
                    prefix ??= string.Empty;

                    var values = _blobs
                        .Keys
                        .Where(name => name.StartsWith(prefix, StringComparison.Ordinal))
                        .Select(name => name[prefix.Length..])
                        .Where(name => name.Length > 0)
                        .Select(name =>
                        {
                            var delimiterIndex = name.IndexOf(delimiter, StringComparison.Ordinal);
                            return delimiterIndex >= 0
                                ? BlobsModelFactory.BlobHierarchyItem(prefix + name[..(delimiterIndex + delimiter.Length)], null)
                                : BlobsModelFactory.BlobHierarchyItem(null, CreateBlobItem(prefix + name, new Dictionary<string, string>(StringComparer.Ordinal)));
                        })
                        .DistinctBy(item => item.IsPrefix ? item.Prefix : item.Blob.Name, StringComparer.Ordinal)
                        .OrderBy(item => item.IsPrefix ? item.Prefix : item.Blob.Name, StringComparer.Ordinal)
                        .ToList();

                    return CreateHierarchyPageable(values);
                });

            Client
                .Setup(container => container.DeleteBlobIfExistsAsync(
                    It.IsAny<string>(),
                    It.IsAny<DeleteSnapshotsOption>(),
                    It.IsAny<BlobRequestConditions>(),
                    It.IsAny<CancellationToken>()))
                .Returns((string blobName, DeleteSnapshotsOption _, BlobRequestConditions? _, CancellationToken __) =>
                {
                    var deleted = _blobs.Remove(blobName);
                    return Task.FromResult(Response.FromValue(deleted, Mock.Of<Response>()));
                });
        }

        public bool Contains(string blobName) => _blobs.ContainsKey(blobName);

        public string GetContent(string blobName) => _blobs[blobName].Content.ToString();

        public int GetDownloadCount(string blobName)
            => _downloadCounts.TryGetValue(blobName, out var count) ? count : 0;

        public void DeleteOnFirstDownload(string blobName)
            => GetOrCreateBlobClient(blobName)
                .Setup(blob => blob.DownloadContentAsync(It.IsAny<CancellationToken>()))
                .Returns((CancellationToken _) =>
                {
                    _blobs.Remove(blobName);
                    throw new RequestFailedException(404, "Blob not found");
                });

        public void SeedBlob(string blobName, BinaryData content, IDictionary<string, string>? metadata = null)
        {
            _blobs[blobName] = new BlobState(content, new Dictionary<string, string>(metadata ?? new Dictionary<string, string>(), StringComparer.Ordinal), CreateEtag(blobName, 0));
        }

        private Mock<BlobClient> GetOrCreateBlobClient(string blobName)
        {
            if (_clients.TryGetValue(blobName, out var existing))
                return existing;

            var client = new Mock<BlobClient>();

            client
                .Setup(blob => blob.DownloadContentAsync(It.IsAny<CancellationToken>()))
                .Returns((CancellationToken _) =>
                {
                    if (!_blobs.TryGetValue(blobName, out var blob))
                        throw new RequestFailedException(404, "Blob not found");

                    _downloadCounts[blobName] = GetDownloadCount(blobName) + 1;

                    var details = BlobsModelFactory.BlobDownloadDetails(
                        metadata: new Dictionary<string, string>(blob.Metadata, StringComparer.Ordinal),
                        eTag: blob.ETag);
                    var result = BlobsModelFactory.BlobDownloadResult(blob.Content, details);
                    return Task.FromResult(Response.FromValue(result, Mock.Of<Response>()));
                });

            client
                .Setup(blob => blob.GetPropertiesAsync(
                    It.IsAny<BlobRequestConditions>(),
                    It.IsAny<CancellationToken>()))
                .Returns((BlobRequestConditions _, CancellationToken _) =>
                {
                    if (!_blobs.TryGetValue(blobName, out var blob))
                        throw new RequestFailedException(404, "Blob not found");

                    var properties = BlobsModelFactory.BlobProperties(eTag: blob.ETag);
                    return Task.FromResult(Response.FromValue(properties, Mock.Of<Response>()));
                });

            client
                .Setup(blob => blob.UploadAsync(It.IsAny<BinaryData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .Returns((BinaryData content, bool overwrite, CancellationToken _) =>
                {
                    if (!overwrite && _blobs.ContainsKey(blobName))
                        throw new RequestFailedException(409, "Blob already exists");

                    PutBlob(blobName, content, metadata: null);
                    return Task.FromResult(Response.FromValue(default(BlobContentInfo), Mock.Of<Response>()));
                });

            client
                .Setup(blob => blob.UploadAsync(It.IsAny<BinaryData>(), It.IsAny<BlobUploadOptions>(), It.IsAny<CancellationToken>()))
                .Returns((BinaryData content, BlobUploadOptions options, CancellationToken _) =>
                {
                    if (options.Conditions?.IfNoneMatch == ETag.All && _blobs.ContainsKey(blobName))
                        throw new RequestFailedException(412, "Blob already exists");

                    if (options.Conditions?.IfMatch is ETag expectedEtag
                        && expectedEtag != default
                        && _blobs.TryGetValue(blobName, out var existing)
                        && existing.ETag != expectedEtag)
                    {
                        throw new RequestFailedException(412, "ETag mismatch");
                    }

                    PutBlob(blobName, content, options.Metadata);
                    return Task.FromResult(Response.FromValue(default(BlobContentInfo), Mock.Of<Response>()));
                });

            _clients[blobName] = client;
            return client;
        }

        private void PutBlob(string blobName, BinaryData content, IDictionary<string, string>? metadata)
        {
            var version = _blobs.TryGetValue(blobName, out var existing) ? existing.Version + 1 : 0;
            _blobs[blobName] = new BlobState(
                content,
                new Dictionary<string, string>(metadata ?? new Dictionary<string, string>(), StringComparer.Ordinal),
                CreateEtag(blobName, version),
                version);
        }

        private static BlobItem CreateBlobItem(string blobName, IDictionary<string, string> metadata, ETag? etag = null)
            => BlobsModelFactory.BlobItem(
                name: blobName,
                deleted: false,
            properties: null,
                versionId: null,
                metadata: new Dictionary<string, string>(metadata, StringComparer.Ordinal));

        private static AsyncPageable<BlobHierarchyItem> CreateHierarchyPageable(List<BlobHierarchyItem> values)
            => AsyncPageable<BlobHierarchyItem>.FromPages(
            [
                Page<BlobHierarchyItem>.FromValues(
                    values,
                    continuationToken: null,
                    Mock.Of<Response>())
            ]);

        private static ETag CreateEtag(string blobName, int version)
            => new($"\"{blobName}:{version}\"");

        private sealed record BlobState(BinaryData Content, Dictionary<string, string> Metadata, ETag ETag, int Version = 0);
    }
}