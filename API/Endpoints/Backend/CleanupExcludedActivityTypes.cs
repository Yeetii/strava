using Microsoft.AspNetCore.Mvc;
using API.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using Shared.Services;
using System.Net;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services.Postgres.Admin;

namespace API.Endpoints.Backend;

public class CleanupExcludedActivityTypes(
    IBackendAdminQueries queries,
    ILogger<CleanupExcludedActivityTypes> logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/cleanup-excluded-activity-types", (HttpContext context, [FromServices] CleanupExcludedActivityTypes endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        string? userId = context.Request.Query["userId"].ToString(); // optional — omit to process all users
        var featureParam = context.Request.Query["feature"].ToString() ?? "all";
        var dryRun = string.Equals(context.Request.Query["dryRun"], "true", StringComparison.OrdinalIgnoreCase);

        var includePaths = featureParam is "all" or "paths";
        var includePeaks = featureParam is "all" or "peaks";
        var includeAreas = featureParam is "all" or "areas";

        // Project id + userId + sportType. Scope to a single user when requested.
        var activities = await queries.GetActivityProjections(userId, cancellationToken);

        // Build activityId → userId maps for each feature's excluded set.
        var excludedFromPaths = activities
            .Where(a => !string.IsNullOrEmpty(a.SportType) && ActivityTypeFilters.ExcludedFromPaths.Contains(a.SportType))
            .ToDictionary(a => a.Id, a => a.UserId, StringComparer.Ordinal);

        var excludedFromPeaks = activities
            .Where(a => !string.IsNullOrEmpty(a.SportType) && ActivityTypeFilters.ExcludedFromPeaks.Contains(a.SportType))
            .ToDictionary(a => a.Id, a => a.UserId, StringComparer.Ordinal);

        var excludedFromAreas = activities
            .Where(a => !string.IsNullOrEmpty(a.SportType) && ActivityTypeFilters.ExcludedFromAreas.Contains(a.SportType))
            .ToDictionary(a => a.Id, a => a.UserId, StringComparer.Ordinal);

        int pathsDeleted = 0, pathsPatched = 0;
        int peaksDeleted = 0, peaksPatched = 0;
        int areasDeleted = 0, areasPatched = 0;

        if (includePaths && excludedFromPaths.Count > 0)
            pathsDeleted = await queries.RemoveExcludedActivityLinks(
                BackendLinkFeature.Paths, excludedFromPaths.Keys, dryRun, cancellationToken);

        if (includePeaks && excludedFromPeaks.Count > 0)
            peaksDeleted = await queries.RemoveExcludedActivityLinks(
                BackendLinkFeature.Peaks, excludedFromPeaks.Keys, dryRun, cancellationToken);

        if (includeAreas && excludedFromAreas.Count > 0)
            areasDeleted = await queries.RemoveExcludedActivityLinks(
                BackendLinkFeature.Areas, excludedFromAreas.Keys, dryRun, cancellationToken);

        // Junction rows are per-activity, so removing an excluded activity always deletes a row;
        // there is no shared document left to "patch" (kept in the response for compatibility).
        logger.LogInformation(
            "CleanupExcludedActivityTypes for {Scope} (dryRun={DryRun}): " +
            "paths deleted={PathsDeleted} patched={PathsPatched}; " +
            "peaks deleted={PeaksDeleted} patched={PeaksPatched}; " +
            "areas deleted={AreasDeleted} patched={AreasPatched}",
            string.IsNullOrWhiteSpace(userId) ? "all users" : $"user {userId}", dryRun,
            pathsDeleted, pathsPatched,
            peaksDeleted, peaksPatched,
            areasDeleted, areasPatched);

        return TypedResults.Ok(new
        {
            userId = string.IsNullOrWhiteSpace(userId) ? "all" : userId,
            dryRun,
            excludedActivities = new
            {
                fromPaths = excludedFromPaths.Count,
                fromPeaks = excludedFromPeaks.Count,
                fromAreas = excludedFromAreas.Count,
            },
            paths = new { deleted = pathsDeleted, patched = pathsPatched },
            peaks = new { deleted = peaksDeleted, patched = peaksPatched },
            areas = new { deleted = areasDeleted, patched = areasPatched },
        });
    }
}
