using Stigar.Api.Endpoints.Tiles;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;

namespace Stigar.Api.Http;

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
                     type.Namespace?.StartsWith("Stigar.Api", StringComparison.Ordinal) == true &&
                     type.GetMethod("Map", BindingFlags.Public | BindingFlags.Static) is not null))
        {
            services.AddTransient(type);
        }
        return services;
    }

    public static IEndpointRouteBuilder MapApiEndpoints(this IEndpointRouteBuilder app)
    {
        Stigar.Api.GetAllPeaksGroups.Map(app);
        Stigar.Api.GetAllSummitedPeaks.Map(app);
        Stigar.Api.GetPeaks.Map(app);
        Stigar.Api.GetSkiDays.Map(app);
        Stigar.Api.GetStreaks.Map(app);
        Stigar.Api.Endpoints.PostLogin.Map(app);
        Stigar.Api.Endpoints.PostLogout.Map(app);
        Stigar.Api.Endpoints.Admin.FetchCountries.Map(app);
        Stigar.Api.Endpoints.Admin.GetAdminStatus.Map(app);
        Stigar.Api.Endpoints.Admin.GetAssembledRaceById.Map(app);
        Stigar.Api.Endpoints.Admin.GetAssembledRaces.Map(app);
        Stigar.Api.Endpoints.Admin.GetCorruptedAdminBoundaries.Map(app);
        Stigar.Api.Endpoints.Admin.GetCountries.Map(app);
        Stigar.Api.Endpoints.Admin.GetOrganizerRedirectCandidates.Map(app);
        Stigar.Api.Endpoints.Admin.GetOrganizerStats.Map(app);
        Stigar.Api.Endpoints.Admin.GetRaceOrganizerById.Map(app);
        Stigar.Api.Endpoints.Admin.GetRaceOrganizers.Map(app);
        Stigar.Api.Endpoints.Admin.GetUserStats.Map(app);
        Stigar.Api.Endpoints.Admin.GetUsersStatus.Map(app);
        Stigar.Api.Endpoints.Admin.MoveOsmFeaturesByZoom.Map(app);
        Stigar.Api.Endpoints.Admin.PostManualGpx.Map(app);
        Stigar.Api.Endpoints.Admin.PostManualMistralScrape.Map(app);
        Stigar.Api.Endpoints.Admin.PostOrganizerRedirect.Map(app);
        Stigar.Api.Endpoints.Admin.PostRaceDiscovery.Map(app);
        Stigar.Api.Endpoints.Admin.PostRaceEvent.Map(app);
        Stigar.Api.Endpoints.Admin.BumpJobEpoch.Map(app);
        Stigar.Api.Endpoints.Admin.RefetchUserActivities.Map(app);
        Stigar.Api.Endpoints.Admin.RefreshUserStravaToken.Map(app);
        Stigar.Api.Endpoints.Admin.RemoveUser.Map(app);
        Stigar.Api.Endpoints.Admin.WipeEmptyAdminBoundaryMarkers.Map(app);
        Stigar.Api.Endpoints.Admin.WipeHangfireQueue.Map(app);
        Stigar.Api.Endpoints.Admin.WipeOsmFeaturesByKind.Map(app);
        Stigar.Api.Endpoints.AdminBoundaries.GetAdminBoundariesByGrid.Map(app);
        Stigar.Api.Endpoints.Aggregates.GetSummitsStats.Map(app);
        Stigar.Api.Endpoints.Backend.BackfillAdminBoundaryMetrics.Map(app);
        Stigar.Api.Endpoints.Backend.BackfillVisitedPathOsmHighwayIds.Map(app);
        Stigar.Api.Endpoints.Backend.BackfillVisitedPathOsmHighwayIds.MapAll(app);
        Stigar.Api.Endpoints.Backend.CleanupExcludedActivityTypes.Map(app);
        Stigar.Api.Endpoints.Backend.PostScrapeOrganizer.Map(app);
        Stigar.Api.Endpoints.Backend.QueueAllScrapeJobsHttp.Map(app);
        Stigar.Api.Endpoints.Backend.QueueAllSummitJobs.Map(app);
        Stigar.Api.Endpoints.Backend.QueueAllVisitedAreasJobs.Map(app);
        Stigar.Api.Endpoints.Backend.QueueAllVisitedPathsJobs.Map(app);
        Stigar.Api.Endpoints.Backend.RefreshStravaToken.Map(app);
        Stigar.Api.Endpoints.Backend.RunSummitsForActivity.Map(app);
        Stigar.Api.Endpoints.Backend.StoreCuratedPeaksGroups.Map(app);
        Stigar.Api.Endpoints.Backend.StravaWebhook.Map(app);
        Stigar.Api.Endpoints.Backend.TestScrapeUrl.Map(app);
        Stigar.Api.Endpoints.GarminCourses.DeleteGarminCourse.Map(app);
        Stigar.Api.Endpoints.GarminCourses.GetGarminCourse.Map(app);
        Stigar.Api.Endpoints.GarminCourses.GetGarminCourseGpx.Map(app);
        Stigar.Api.Endpoints.GarminCourses.GetGarminCourses.Map(app);
        Stigar.Api.Endpoints.GarminCourses.PostGarminCourse.Map(app);
        Stigar.Api.Endpoints.GarminCourses.PostGarminDeviceMessages.Map(app);
        Stigar.Api.Endpoints.GarminCourses.PutGarminCourse.Map(app);
        Stigar.Api.Endpoints.Mcp.PostMcp.Map(app);
        Stigar.Api.Endpoints.OpenApi.GetSourceOpenApiDocument.Map(app);
        Stigar.Api.Endpoints.Paths.GetVisitedPaths.Map(app);
        Stigar.Api.Endpoints.Peaks.GetGridIndices.Map(app);
        Stigar.Api.Endpoints.Peaks.GetPeaksByGrid.Map(app);
        Stigar.Api.Endpoints.ProtectedAreas.GetProtectedAreasByGrid.Map(app);
        Stigar.Api.Endpoints.ProtectedAreas.GetVisitedAreas.Map(app);
        Stigar.Api.Endpoints.Routing.PostGraphHopperRoute.Map(app);
        Stigar.Api.Endpoints.StravaRoutes.GetStravaRouteGpx.Map(app);
        Stigar.Api.Endpoints.StravaRoutes.GetStravaRoutes.Map(app);
        Stigar.Api.Endpoints.Tiles.GetHighwayTilePbf.Map(app);
        Stigar.Api.Endpoints.User.DeleteAccount.Map(app);
        Stigar.Api.Endpoints.User.GetActivities.Map(app);
        Stigar.Api.Endpoints.User.GetActivitiesByGrid.Map(app);
        Stigar.Api.Endpoints.User.GetActivityStatuses.Map(app);
        Stigar.Api.Endpoints.User.GetCurrentUser.Map(app);
        Stigar.Api.Endpoints.User.GetUserSync.Map(app);
        app.MapGetTrailsPmtiles(((IEndpointRouteBuilder)app).ServiceProvider.GetRequiredService<IConfiguration>());
        return app;
    }
}
