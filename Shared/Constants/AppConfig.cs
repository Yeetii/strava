namespace Shared.Constants;

public static class AppConfig
{
    public const string TippecanoeBinaryPath = "TippecanoeBinaryPath";
    public const string TileJoinBinaryPath = "TileJoinBinaryPath";
    public const string BlobShardZoom = "BlobShardZoom";
    public const string BlobShardBufferMeters = "BlobShardBufferMeters";

    // Organizer store selection. Blob keeps the existing behaviour; FileSystem
    // is the self-hosted store. Opt in explicitly with OrganizerStore:Provider.
    public const string OrganizerStoreProvider = "OrganizerStore:Provider";
    public const string RaceDataDir = "OrganizerStore:RaceDataDir";

    // Shard/index store selection. Blob keeps the existing behaviour; FileSystem
    // is the self-hosted store. Opt in explicitly with ShardStore:Provider.
    public const string ShardStoreProvider = "ShardStore:Provider";
    public const string ShardStoreRootDir = "ShardStore:RootDir";

    // Shared Redis (Hangfire transport and the race-build geocoding cache).
    public const string RedisConnectionString = "RedisConnectionString";

    // Race tile output and weekly build schedule.
    public const string RaceTilesOutputPath = "RaceTiles:OutputPath";
    public const string RaceTilesWeeklyCron = "RaceTiles:WeeklyCron";
    public const string DefaultRaceTilesOutputPath = "/data/races/tiles/trails.pmtiles";
    public const string DefaultRaceTilesWeeklyCron = "0 5 * * 1";
}

public static class OrganizerStoreProviders
{
    public const string Blob = "Blob";
    public const string FileSystem = "FileSystem";
}

public static class ShardStoreProviders
{
    public const string Blob = "Blob";
    public const string FileSystem = "FileSystem";
}

public static class BlobContainerNames
{
    public const string RaceOrganizers = "race-organizers";
    public const string RaceTiles = "race-tiles";
    public const string HighwaysShards = "highways-shards-v2";
    public const string PoisShards = "pois-shards";
    public const string BuildingsShards = "buildings-shards";
    public const string SharedDictionaries = "shared-dictionaries";
}
