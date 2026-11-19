using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Stigar.Api.Http;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using UserDocument = Stigar.Core.Models.User;

namespace Stigar.Api.Endpoints.Admin;

public record AdminUserActivityProcessingSummary(
    int TotalActivities,
    int WithProcessingStatus,
    int ProcessedSummitedPeaks,
    int ProcessedVisitedPaths,
    int ProcessedVisitedAreas,
    int WithErrors,
    DateTime? LastUpdatedAtUtc);

public record AdminRecentUserActivityStatus(
    string ActivityId,
    string ActivityName,
    DateTime StartDateLocal,
    ActivityProcessingStatus ProcessingStatus);

public record AdminStravaAuthStatus(
    bool HasRefreshToken,
    bool HasAccessToken,
    DateTime? AccessTokenExpiresAtUtc,
    bool AccessTokenExpired,
    string? RawScope,
    IReadOnlyList<string> Scopes,
    bool HasRead,
    bool HasReadAll,
    bool HasActivityRead,
    bool HasActivityReadAll,
    bool HasActivityWrite);

public record AdminUserStats(
    string UserId,
    string? Username,
    string? FirstName,
    string? LastName,
    int ActivityCount,
    DateTime? LastLoggedInAtUtc,
    AdminStravaAuthStatus StravaAuthStatus,
    StravaSyncStatus SyncStatus,
    AdminUserActivityProcessingSummary ActivityProcessingSummary,
    IReadOnlyList<AdminRecentUserActivityStatus> RecentActivities);

public class GetUserStats(
    IUserRepository usersRepository,
    IActivityRepository activitiesRepository,
    ISessionRepository sessionsRepository,
    IConfiguration configuration)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/users/{userId}/stats",
            (HttpContext context, [FromServices] GetUserStats endpoint, string userId) => endpoint.Handle(context, userId));

    public async Task<IResult> Handle(HttpContext context, string userId)
    {
        if (!context.IsAdminAuthorized(configuration))
        {
            return TypedResults.Unauthorized();
        }

        var user = await usersRepository.GetById(userId);
        if (user == null)
        {
            return TypedResults.NotFound();
        }

        var activities = (await activitiesRepository.GetAllByUser(userId))
            .OrderByDescending(activity => activity.StartDateLocal)
            .ToArray();
        var lastLoggedInAtUtc = (await sessionsRepository.GetByUserId(userId))
            .Select(session => (DateTime?)session.CreatedAtUtc)
            .Max();

        var processingStatuses = activities
            .Select(activity => activity.ProcessingStatus)
            .Where(status => status != null)
            .Select(status => status!)
            .ToArray();

        var summary = new AdminUserActivityProcessingSummary(
            TotalActivities: activities.Length,
            WithProcessingStatus: processingStatuses.Length,
            ProcessedSummitedPeaks: processingStatuses.Count(status => status.SummitedPeaks),
            ProcessedVisitedPaths: processingStatuses.Count(status => status.VisitedPaths),
            ProcessedVisitedAreas: processingStatuses.Count(status => status.VisitedAreas),
            WithErrors: processingStatuses.Count(status => !string.IsNullOrWhiteSpace(status.LastProcessingError)),
            LastUpdatedAtUtc: processingStatuses
                .Select(status => status.LastUpdatedAtUtc)
                .Where(updatedAt => updatedAt.HasValue)
                .Max());

        return TypedResults.Ok(new AdminUserStats(
            UserId: user.Id,
            Username: user.UserName,
            FirstName: user.FirstName,
            LastName: user.LastName,
            ActivityCount: activities.Length,
            LastLoggedInAtUtc: lastLoggedInAtUtc,
            StravaAuthStatus: BuildStravaAuthStatus(user),
            SyncStatus: user.SyncStatus ?? UserSyncStatusService.CreateDefaultStatus(),
            ActivityProcessingSummary: summary,
            RecentActivities: activities
                .Take(10)
                .Select(activity => new AdminRecentUserActivityStatus(
                    ActivityId: activity.Id,
                    ActivityName: activity.Name,
                    StartDateLocal: activity.StartDateLocal,
                    ProcessingStatus: activity.ProcessingStatus ?? new ActivityProcessingStatus()))
                .ToArray()));
    }

    public static AdminStravaAuthStatus BuildStravaAuthStatus(UserDocument user)
    {
        var scopes = (user.StravaScope ?? string.Empty)
            .Split([',', ' '], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(scope => scope, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var scopeSet = scopes.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var expiresAtUtc = user.TokenExpiresAt > 0
            ? DateTimeOffset.FromUnixTimeSeconds(user.TokenExpiresAt).UtcDateTime
            : (DateTime?)null;

        return new AdminStravaAuthStatus(
            HasRefreshToken: !string.IsNullOrWhiteSpace(user.RefreshToken),
            HasAccessToken: !string.IsNullOrWhiteSpace(user.AccessToken),
            AccessTokenExpiresAtUtc: expiresAtUtc,
            AccessTokenExpired: expiresAtUtc.HasValue && expiresAtUtc.Value <= DateTime.UtcNow,
            RawScope: user.StravaScope,
            Scopes: scopes,
            HasRead: scopeSet.Contains("read"),
            HasReadAll: scopeSet.Contains("read_all"),
            HasActivityRead: scopeSet.Contains("activity:read"),
            HasActivityReadAll: scopeSet.Contains("activity:read_all"),
            HasActivityWrite: scopeSet.Contains("activity:write"));
    }
}
