namespace Shared.Constants
{
    public static class DatabaseConfig
    {
        public static readonly string[] AllContainers = typeof(DatabaseConfig)
            .GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Where(field => field is { IsLiteral: true, FieldType: not null }
                && field.FieldType == typeof(string)
                && field.Name.EndsWith("Container", StringComparison.Ordinal))
            .Select(field => (string)field.GetRawConstantValue()!)
            .OrderBy(containerName => containerName)
            .ToArray();

        public const string CosmosDb = "db";
        public const string SummitedPeaksContainer = "summitedPeaks";
        public const string ActivitiesContainer = "activities";
        public const string UsersContainer = "users";
        public const string UserSyncItemsContainer = "userSyncItems";
        public const string SessionsContainer = "sessions";
        public const string VisitedPathsContainer = "visitedPaths";
        public const string VisitedAreasContainer = "visitedAreas";
        public const string PeaksGroupsContainer = "peaksGroups";
        public const string RaceOrganizersContainer = "raceOrganizers";

        // Shared cache for all Overpass-derived features (peaks, paths, protected areas, admin boundaries).
        // Requires the container to have DefaultTimeToLive = 2592000 (30 days) configured in Cosmos.
        public const string OsmFeaturesContainer = "osmFeatures";
    }
}
