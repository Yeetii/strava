using System.Text.Json;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class FileSystemOrganizerStoreTests : IDisposable
{
    private readonly string _root;
    private readonly IOrganizerStore _store;

    public FileSystemOrganizerStoreTests()
    {
        _root = Path.Combine(Path.GetTempPath(), nameof(FileSystemOrganizerStoreTests), Guid.NewGuid().ToString("N"));
        _store = new FileSystemOrganizerStore(_root, NullLoggerFactory.Instance);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }

    [Fact]
    public void Constructor_RejectsBlankRoot()
    {
        Assert.Throws<ArgumentException>(() => new FileSystemOrganizerStore("   ", NullLoggerFactory.Instance));
    }

    [Fact]
    public async Task WriteAsync_And_GetByIdAsync_RoundTrip()
    {
        var doc = new RaceOrganizerDocument { Id = "example.com", Url = "https://example.com/" };

        await _store.WriteAsync(doc);

        Assert.Equal("example.com", doc.Id);
        var loaded = await _store.GetByIdAsync("example.com");
        Assert.NotNull(loaded);
        Assert.Equal("https://example.com/", loaded!.Url);
        Assert.True(File.Exists(Path.Combine(_root, "example.com", "organizer.json")));
    }

    [Fact]
    public async Task WriteAsync_AllowsTildeAndDotInKey()
    {
        const string key = "runsignup.com~Race~TX~Longview~LongviewTrailRunsSpring";

        await _store.WriteAsync(new RaceOrganizerDocument { Id = key, Url = "https://runsignup.com/Race/TX/Longview/LongviewTrailRunsSpring" });

        var loaded = await _store.GetByIdAsync(key);
        Assert.NotNull(loaded);
        Assert.Equal(key, loaded!.Id);
    }

    [Fact]
    public async Task WriteAsync_RejectsTraversalKey()
    {
        var doc = new RaceOrganizerDocument { Id = "../escape", Url = "https://escape.example/" };

        await Assert.ThrowsAsync<ArgumentException>(() => _store.WriteAsync(doc));
    }

    [Fact]
    public async Task GetByIdAsync_ReturnsNullForMissingDocument()
    {
        Assert.Null(await _store.GetByIdAsync("missing.example"));
    }

    [Fact]
    public async Task WriteDiscoveryAsync_MergesBySource()
    {
        await _store.WriteAsync(new RaceOrganizerDocument { Id = "discover.example", Url = "https://discover.example/" });

        await _store.WriteDiscoveryAsync("discover.example", "https://discover.example/", "manual", [NewDiscovery("first")]);
        await _store.WriteDiscoveryAsync("discover.example", "https://discover.example/", "utmb", [NewDiscovery("second")]);

        var doc = await _store.GetByIdAsync("discover.example");
        Assert.NotNull(doc);
        Assert.Equal(2, doc!.Discovery!.Count);
        Assert.Equal("first", Assert.Single(doc.Discovery["manual"]).Name);
        Assert.Equal("second", Assert.Single(doc.Discovery["utmb"]).Name);

        // Writing the same source again overwrites that source's list.
        await _store.WriteDiscoveryAsync("discover.example", "https://discover.example/", "manual", [NewDiscovery("replacement")]);
        doc = await _store.GetByIdAsync("discover.example");
        Assert.Equal("replacement", Assert.Single(doc!.Discovery!["manual"]).Name);
    }

    [Fact]
    public async Task WriteDiscoveriesAsync_MergesDuplicateKeys()
    {
        await _store.WriteAsync(new RaceOrganizerDocument { Id = "batch.example", Url = "https://batch.example/" });

        await _store.WriteDiscoveriesAsync("manual",
        [
            ("batch.example", "https://batch.example/", [NewDiscovery("a")]),
            ("batch.example", "https://batch.example/", [NewDiscovery("b")]),
        ]);

        var doc = await _store.GetByIdAsync("batch.example");
        Assert.NotNull(doc);
        Assert.Equal(2, doc!.Discovery!["manual"].Count);
        Assert.Equal(new[] { "a", "b" }, doc.Discovery["manual"].Select(d => d.Name));
    }

    [Fact]
    public async Task WriteScraperOutputAsync_And_PatchScraperPropertiesAsync_PreservesRoutes()
    {
        await _store.WriteAsync(new RaceOrganizerDocument { Id = "scrape.example", Url = "https://scrape.example/" });
        await _store.WriteScraperOutputAsync("scrape.example", "bfs", new ScraperOutput
        {
            ScrapedAtUtc = "2026-04-01T00:00:00Z",
            WebsiteUrl = "https://old.example/",
            Routes = [new ScrapedRouteOutput { Name = "route-1" }],
        });

        await _store.PatchScraperPropertiesAsync("scrape.example", "bfs", new ScraperOutput
        {
            ScrapedAtUtc = "2026-05-01T00:00:00Z",
            WebsiteUrl = "https://new.example/",
            ExtractedName = "Renamed",
        });

        var doc = await _store.GetByIdAsync("scrape.example");
        Assert.NotNull(doc);
        var scraper = doc!.Scrapers!["bfs"];
        Assert.Equal("2026-05-01T00:00:00Z", scraper.ScrapedAtUtc);
        Assert.Equal("https://new.example/", scraper.WebsiteUrl);
        Assert.Equal("Renamed", scraper.ExtractedName);
        Assert.Equal("route-1", Assert.Single(scraper.Routes!).Name);
    }

    [Fact]
    public async Task PatchLastScrapedAsync_DrivesFreshnessQuery()
    {
        await _store.WriteAsync(new RaceOrganizerDocument { Id = "fresh.example", Url = "https://fresh.example/" });
        await _store.WriteAsync(new RaceOrganizerDocument { Id = "stale.example", Url = "https://stale.example/" });
        await _store.WriteAsync(new RaceOrganizerDocument { Id = "never.example", Url = "https://never.example/" });

        await _store.PatchLastScrapedAsync("fresh.example", "2026-05-01T00:00:00Z", null);
        await _store.PatchLastScrapedAsync("stale.example", "2026-04-01T00:00:00Z", new Dictionary<string, ScraperOutputHashes>
        {
            ["bfs"] = new ScraperOutputHashes { PropertiesHash = "p", RoutesHash = "r" },
        });

        var due = await _store.GetIdsDueForAutomaticScrapeAsync(new DateTime(2026, 4, 15, 0, 0, 0, DateTimeKind.Utc));

        Assert.DoesNotContain("fresh.example", due);
        Assert.Contains("stale.example", due);
        Assert.Contains("never.example", due);

        var staleDoc = await _store.GetByIdAsync("stale.example");
        Assert.Equal("p", staleDoc!.ScraperHashes!["bfs"].PropertiesHash);
    }

    [Fact]
    public async Task SetRedirectAsync_WipesSourceDirectoryAndResolvesForWrites()
    {
        await _store.WriteAsync(new RaceOrganizerDocument { Id = "alias.example", Url = "https://alias.example/" });
        var aliasRaces = Path.Combine(_root, "alias.example", "races");
        Directory.CreateDirectory(aliasRaces);
        File.WriteAllText(Path.Combine(aliasRaces, "stale.json"), "{}");

        await _store.SetRedirectAsync("alias.example", "canonical.example");

        Assert.False(File.Exists(Path.Combine(_root, "alias.example", "organizer.json")));
        Assert.False(File.Exists(Path.Combine(aliasRaces, "stale.json")));
        Assert.True(File.Exists(Path.Combine(_root, "alias.example", "redirect.txt")));
        Assert.Equal("canonical.example", await _store.ResolveOrganizerKeyAsync("alias.example"));

        await _store.WriteDiscoveryAsync("alias.example", "https://alias.example/", "manual", [NewDiscovery("via-alias")]);

        var viaAlias = await _store.GetByIdAsync("alias.example");
        Assert.NotNull(viaAlias);
        Assert.Equal("canonical.example", viaAlias!.Id);
        Assert.Equal("via-alias", Assert.Single(viaAlias.Discovery!["manual"]).Name);
    }

    [Fact]
    public async Task SetRedirectAsync_RejectsSameSourceAndTarget()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(() => _store.SetRedirectAsync("same.example", "same.example"));
    }

    [Fact]
    public async Task ResolveOrganizerKeyAsync_DetectsRedirectCycles()
    {
        WriteRedirect("a.example", "b.example");
        WriteRedirect("b.example", "a.example");

        await Assert.ThrowsAsync<InvalidOperationException>(() => _store.ResolveOrganizerKeyAsync("a.example"));
    }

    [Fact]
    public async Task UploadRouteGpxAsync_WritesRootRelativeFile()
    {
        await _store.UploadRouteGpxAsync("routes.example/routes/trail.gpx", [1, 2, 3]);

        var path = Path.Combine(_root, "routes.example", "routes", "trail.gpx");
        Assert.True(File.Exists(path));
        Assert.Equal(new byte[] { 1, 2, 3 }, await File.ReadAllBytesAsync(path));
    }

    [Fact]
    public async Task UploadRouteGpxAsync_RejectsTraversal()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => _store.UploadRouteGpxAsync("../escape.gpx", [1]));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.UploadRouteGpxAsync("/absolute/path.gpx", [1]));
    }

    [Fact]
    public async Task WriteAssembledRacesAsync_WritesMarker_SkipsUnchanged_DeletesStale()
    {
        var race = NewRace("marker.example-0");

        var first = await _store.WriteAssembledRacesAsync("tiles.example", [race]);

        Assert.Equal(1, first.OrganizerWrites);
        Assert.Equal(1, first.RaceUploads);
        Assert.Equal(0, first.RaceDeletes);
        Assert.Equal(0, first.RaceUnchanged);

        var path = Path.Combine(_root, "tiles.example", "races", "marker.example-0.json");
        Assert.True(File.Exists(path));
        var marker = JsonSerializer.Deserialize<StoredFeature>(
            await File.ReadAllBytesAsync(path),
            new JsonSerializerOptions(JsonSerializerDefaults.Web));
        Assert.NotNull(marker);
        Assert.IsType<Point>(marker!.Geometry);
        Assert.Equal("Race name", marker.Properties["name"].ToString());

        var second = await _store.WriteAssembledRacesAsync("tiles.example", [race]);
        Assert.Equal(0, second.RaceUploads);
        Assert.Equal(1, second.RaceUnchanged);

        var assembled = await _store.GetAssembledRaceAsync("tiles.example", "marker.example-0");
        Assert.NotNull(assembled);

        var third = await _store.WriteAssembledRacesAsync("tiles.example", []);
        Assert.Equal(1, third.RaceDeletes);
        Assert.False(File.Exists(path));
    }

    [Fact]
    public async Task Streams_ReturnExpectedData()
    {
        await _store.WriteAsync(new RaceOrganizerDocument
        {
            Id = "stream.example",
            Url = "https://stream.example/",
            LastScrapedUtc = "2026-04-01T00:00:00Z",
            Discovery = new Dictionary<string, List<SourceDiscovery>> { ["manual"] = [NewDiscovery("discovered")] },
        });
        WriteRedirect("alias.example", "stream.example");
        await _store.UploadRouteGpxAsync("stream.example/routes/a.gpx", [1]);
        await _store.WriteAssembledRacesAsync("stream.example", [NewRace("assembled.example-0")]);

        var all = new List<string>();
        await foreach (var doc in _store.StreamAllAsync())
            all.Add(doc.Id);
        Assert.Equal(["stream.example"], all);
        Assert.Equal(1, _store.GetLastOrganizerReadMetricsSnapshot().StreamedDocuments);

        var withSource = new List<OrganizerDocumentStreamItem>();
        await foreach (var item in _store.StreamAllWithSourceAsync())
            withSource.Add(item);
        Assert.Equal("stream.example", Assert.Single(withSource).Document.Id);
        Assert.Equal(OrganizerDocumentSource.FileSystem, Assert.Single(withSource).Source);

        var identities = new List<OrganizerBlobIdentity>();
        await foreach (var identity in _store.StreamIdentitiesAsync())
            identities.Add(identity);
        Assert.Equal("stream.example", Assert.Single(identities).Id);

        var metadata = new List<OrganizerBlobMetadataDocument>();
        await foreach (var item in _store.StreamMetadataWithoutGeometriesAsync())
            metadata.Add(item);
        Assert.Equal("stream.example", Assert.Single(metadata).Id);
        Assert.Equal("discovered", Assert.Single(Assert.Single(metadata).Discovery!["manual"]).Name);

        var redirects = new List<OrganizerRedirectEntry>();
        await foreach (var redirect in _store.StreamRedirectsAsync())
            redirects.Add(redirect);
        Assert.Equal("alias.example", Assert.Single(redirects).SourceOrganizerKey);
        Assert.Equal("stream.example", Assert.Single(redirects).TargetOrganizerKey);

        var assembled = new List<(string OrganizerKey, StoredFeature Race)>();
        await foreach (var item in _store.StreamAssembledRacesAsync())
            assembled.Add(item);
        Assert.Equal("stream.example", Assert.Single(assembled).OrganizerKey);
        Assert.Equal("assembled.example-0", Assert.Single(assembled).Race.FeatureId);

        var filtered = new List<(string OrganizerKey, StoredFeature Race)>();
        await foreach (var item in _store.StreamAssembledRacesAsync("stream.example"))
            filtered.Add(item);
        Assert.Single(filtered);

        var counts = await _store.GetCountsAsync();
        Assert.Equal(1, counts.OrganizerCount);
        Assert.Equal(1, counts.AssembledRaceCount);
    }

    [Fact]
    public async Task AtomicWrites_LeaveNoTempFiles()
    {
        await _store.WriteAsync(new RaceOrganizerDocument { Id = "atomic.example", Url = "https://atomic.example/" });
        await _store.WriteDiscoveryAsync("atomic.example", "https://atomic.example/", "manual", [NewDiscovery("d")]);
        await _store.UploadRouteGpxAsync("atomic.example/routes/a.gpx", [1, 2]);
        await _store.WriteAssembledRacesAsync("atomic.example", [NewRace("atomic.example-0")]);
        WriteRedirect("alias.example", "atomic.example");

        Assert.Empty(Directory.EnumerateFiles(_root, "*.tmp", SearchOption.AllDirectories));
    }

    private void WriteRedirect(string source, string target)
    {
        var directory = Path.Combine(_root, source);
        Directory.CreateDirectory(directory);
        File.WriteAllText(Path.Combine(directory, "redirect.txt"), target + "\n", new System.Text.UTF8Encoding(false));
    }

    private static SourceDiscovery NewDiscovery(string name) => new()
    {
        DiscoveredAtUtc = "2026-04-30T12:00:00Z",
        Name = name,
        SourceUrls = ["https://discovery.example/race"],
    };

    private static StoredFeature NewRace(string featureId) => new()
    {
        Id = $"race:{featureId}",
        FeatureId = featureId,
        Kind = FeatureKinds.Race,
        X = 1,
        Y = 2,
        Zoom = RaceAssembler.DefaultZoom,
        Geometry = new Point(new Position(12, 34)),
        Properties = new Dictionary<string, dynamic> { ["name"] = "Race name" },
    };
}
