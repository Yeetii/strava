namespace Shared.Constants
{
    public static class ServiceBusConfig
    {
        public static readonly string[] NonTimeCriticalQueues = [
            CalculateSummitsJobs,
            CalculateVisitedPathsJobs,
            CalculateVisitedAreasJobs,
            ActivityDeleteJobs,
            AccountDeleteJobs,
            EnrichAdminBoundaryJobs,
            RaceDiscoveryJobs,
            ScrapeRace
        ];

        public const string CalculateSummitsJobs = "calculateSummitsJobs";
        public const string CalculateVisitedPathsJobs = "calculateVisitedPathsJobs";
        public const string CalculateVisitedAreasJobs = "calculateVisitedAreasJobs";
        public const string ActivitiesFetchJobs = "activitiesfetchjobs";
        public const string ActivityFetchJobs = "activityFetchJobs";
        public const string ActivityProcessed = "activityprocessed";
        public const string ActivityDeleteJobs = "activityDeleteJobs";
        public const string ScrapeRace = "scrapeRace";
        public const string RaceDiscoveryJobs = "raceDiscoveryJobs";
        public const string MistralScrapeJobs = "mistralScrape";
        public const string EnrichAdminBoundaryJobs = "enrichAdminBoundaryJobs";
        public const string AccountDeleteJobs = "accountDeleteJobs";
    }
}
