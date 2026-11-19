using API.Endpoints.Tiles;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;

namespace API.Http;

/// <summary>Central registration of every native minimal API endpoint.</summary>
public static class EndpointMappings
{
    /// <summary>
    /// Registers endpoint classes so minimal API delegates can resolve them with
    /// [FromServices]. Discovery is limited to endpoint types that expose a Map
    /// method; it is DI registration only, never HTTP route mapping.
    /// </summary>
    public static IServiceCollection AddApiEndpoints(this IServiceCollection services)
    {
        var assembly = typeof(EndpointMappings).Assembly;
        foreach (var type in assembly.GetTypes().Where(type =>
                     type.IsClass && !type.IsAbstract &&
                     type.Namespace?.StartsWith("API", StringComparison.Ordinal) == true &&
                     type.GetMethod("Map", BindingFlags.Public | BindingFlags.Static) is not null))
        {
            services.AddTransient(type);
        }
        return services;
    }

    public static IEndpointRouteBuilder MapApiEndpoints(this IEndpointRouteBuilder app)
    {
        API.GetAllPeaksGroups.Map(app);
        API.GetAllSummitedPeaks.Map(app);
        API.GetPeaks.Map(app);
        API.GetSkiDays.Map(app);
        API.GetStreaks.Map(app);
        API.Endpoints.PostLogin.Map(app);
        API.Endpoints.PostLogout.Map(app);
        API.Endpoints.Admin.FetchCountries.Map(app);
        API.Endpoints.Admin.GetAdminStatus.Map(app);
        API.Endpoints.Admin.GetAssembledRaceById.Map(app);
        API.Endpoints.Admin.GetAssembledRaces.Map(app);
        API.Endpoints.Admin.GetCorruptedAdminBoundaries.Map(app);
        API.Endpoints.Admin.GetCountries.Map(app);
        API.Endpoints.Admin.GetOrganizerRedirectCandidates.Map(app);
        API.Endpoints.Admin.GetOrganizerStats.Map(app);
        API.Endpoints.Admin.GetRaceOrganizerById.Map(app);
        API.Endpoints.Admin.GetRaceOrganizers.Map(app);
        API.Endpoints.Admin.GetUserStats.Map(app);
        API.Endpoints.Admin.GetUsersStatus.Map(app);
        API.Endpoints.Admin.MoveOsmFeaturesByZoom.Map(app);
        API.Endpoints.Admin.PostManualGpx.Map(app);
        API.Endpoints.Admin.PostManualMistralScrape.Map(app);
        API.Endpoints.Admin.PostOrganizerRedirect.Map(app);
        API.Endpoints.Admin.PostRaceDiscovery.Map(app);
        API.Endpoints.Admin.PostRaceEvent.Map(app);
        API.Endpoints.Admin.QueueActivityJobs.Map(app);
        API.Endpoints.Admin.QueueUnprocessedActivityJobs.Map(app);
        API.Endpoints.Admin.ReEnrichAdminBoundaries.Map(app);
        API.Endpoints.Admin.RefetchUserActivities.Map(app);
        API.Endpoints.Admin.RefreshUserStravaToken.Map(app);
        API.Endpoints.Admin.RemoveUser.Map(app);
        API.Endpoints.Admin.WipeEmptyAdminBoundaryMarkers.Map(app);
        API.Endpoints.Admin.WipeOsmFeaturesByKind.Map(app);
        API.Endpoints.AdminBoundaries.GetAdminBoundariesByGrid.Map(app);
        API.Endpoints.Aggregates.GetSummitsStats.Map(app);
        API.Endpoints.Backend.BackfillAdminBoundaryMetrics.Map(app);
        API.Endpoints.Backend.BackfillVisitedPathOsmHighwayIds.Map(app);
        API.Endpoints.Backend.BackfillVisitedPathOsmHighwayIds.MapAll(app);
        API.Endpoints.Backend.CleanupExcludedActivityTypes.Map(app);
        API.Endpoints.Backend.PostScrapeOrganizer.Map(app);
        API.Endpoints.Backend.QueueAllScrapeJobsHttp.Map(app);
        API.Endpoints.Backend.QueueAllSummitJobs.Map(app);
        API.Endpoints.Backend.QueueAllVisitedAreasJobs.Map(app);
        API.Endpoints.Backend.QueueAllVisitedPathsJobs.Map(app);
        API.Endpoints.Backend.RefreshStravaToken.Map(app);
        API.Endpoints.Backend.RunSummitsForActivity.Map(app);
        API.Endpoints.Backend.StoreCuratedPeaksGroups.Map(app);
        API.Endpoints.Backend.StravaWebhook.Map(app);
        API.Endpoints.Backend.TestScrapeUrl.Map(app);
        API.Endpoints.GarminCourses.DeleteGarminCourse.Map(app);
        API.Endpoints.GarminCourses.GetGarminCourse.Map(app);
        API.Endpoints.GarminCourses.GetGarminCourseGpx.Map(app);
        API.Endpoints.GarminCourses.GetGarminCourses.Map(app);
        API.Endpoints.GarminCourses.PostGarminCourse.Map(app);
        API.Endpoints.GarminCourses.PostGarminDeviceMessages.Map(app);
        API.Endpoints.GarminCourses.PutGarminCourse.Map(app);
        API.Endpoints.Mcp.PostMcp.Map(app);
        API.Endpoints.OpenApi.GetSourceOpenApiDocument.Map(app);
        API.Endpoints.Paths.GetVisitedPaths.Map(app);
        API.Endpoints.Peaks.GetGridIndices.Map(app);
        API.Endpoints.Peaks.GetPeaksByGrid.Map(app);
        API.Endpoints.ProtectedAreas.GetProtectedAreasByGrid.Map(app);
        API.Endpoints.ProtectedAreas.GetVisitedAreas.Map(app);
        API.Endpoints.Routing.PostGraphHopperRoute.Map(app);
        API.Endpoints.StravaRoutes.GetStravaRouteGpx.Map(app);
        API.Endpoints.StravaRoutes.GetStravaRoutes.Map(app);
        API.Endpoints.Tiles.GetHighwayTilePbf.Map(app);
        API.Endpoints.User.DeleteAccount.Map(app);
        API.Endpoints.User.GetActivities.Map(app);
        API.Endpoints.User.GetActivitiesByGrid.Map(app);
        API.Endpoints.User.GetActivityStatuses.Map(app);
        API.Endpoints.User.GetCurrentUser.Map(app);
        API.Endpoints.User.GetUserSync.Map(app);
        app.MapGetTrailsPmtiles(((IEndpointRouteBuilder)app).ServiceProvider.GetRequiredService<IConfiguration>());
        return app;
    }
}
