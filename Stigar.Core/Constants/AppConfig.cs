namespace Stigar.Core.Constants;

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

    // Dedicated Hangfire queue: only the pmtiles worker serves it, so the
    // API dashboard can enqueue the job but Backend workers cannot pick it up.
    public const string RaceTilesQueueName = "pmtiles";

    // Dedicated Hangfire queue for the Backend polling recurring jobs. Only the
    // schedule worker serves it, so other hosts sharing the store cannot activate
    // jobs whose implementations are registered only in the Backend assembly.
    public const string BackendPollingQueueName = "backend-polling";

    // Dedicated queue for native IRecurringJob cron invocations. Without an
    // explicit queue Hangfire sends them to "default", which no worker serves.
    public const string RecurringJobsQueueName = "recurring-jobs";
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
