using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;
using UserDocument = Shared.Models.User;

namespace API.Endpoints.Admin;

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

public record AdminUserStats(
    string UserId,
    string? Username,
    string? FirstName,
    string? LastName,
    int ActivityCount,
    DateTime? LastLoggedInAtUtc,
    StravaSyncStatus SyncStatus,
    AdminUserActivityProcessingSummary ActivityProcessingSummary,
    IReadOnlyList<AdminRecentUserActivityStatus> RecentActivities);

internal sealed record UserDetailProjection(
    string Id,
    string? UserName,
    string? FirstName,
    string? LastName,
    StravaSyncStatus? SyncStatus);

internal sealed record ActivityDetailProjection(
    string Id,
    string Name,
    DateTime StartDateLocal,
    ActivityProcessingStatus? ProcessingStatus);

internal sealed record AdminUserLastLoginProjection(
    string UserId,
    DateTime? LastLoggedInAtUtc);

public class GetUserStats(
    CollectionClient<UserDocument> usersCollection,
    CollectionClient<Activity> activitiesCollection,
    CollectionClient<Session> sessionsCollection,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "userId", In = ParameterLocation.Path, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(AdminUserStats))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NotFound)]
    [Function(nameof(GetUserStats))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/users/{userId}/stats")] HttpRequestData req,
        string userId)
    {
        if (!IsAuthorized(req))
        {
            return req.CreateResponse(HttpStatusCode.Unauthorized);
        }

        var userTask = usersCollection.GetByIdMaybe(userId, new PartitionKey(userId));
        var activityCountTask = activitiesCollection.ExecuteQueryAsync<int>(
            new QueryDefinition("SELECT VALUE COUNT(1) FROM c WHERE c.userId = @userId")
                .WithParameter("@userId", userId));
        var lastLoginTask = sessionsCollection.ExecuteQueryAsync<AdminUserLastLoginProjection>(
            new QueryDefinition("SELECT c.userId AS userId, MAX(c.createdAtUtc) AS lastLoggedInAtUtc FROM c WHERE c.userId = @userId AND IS_DEFINED(c.createdAtUtc) GROUP BY c.userId")
                .WithParameter("@userId", userId));
        var activityDetailsTask = activitiesCollection.ExecuteQueryAsync<ActivityDetailProjection>(
            new QueryDefinition("SELECT c.id, c.name, c.startDateLocal, c.processingStatus FROM c WHERE c.userId = @userId ORDER BY c.startDateLocal DESC")
                .WithParameter("@userId", userId));

        await Task.WhenAll(userTask, activityCountTask, lastLoginTask, activityDetailsTask);

        var user = userTask.Result;
        if (user == null)
        {
            return req.CreateResponse(HttpStatusCode.NotFound);
        }

        var activities = activityDetailsTask.Result.ToArray();
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

        var lastLoggedInAtUtc = lastLoginTask.Result.FirstOrDefault()?.LastLoggedInAtUtc;

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new AdminUserStats(
            UserId: user.Id,
            Username: user.UserName,
            FirstName: user.FirstName,
            LastName: user.LastName,
            ActivityCount: activityCountTask.Result.FirstOrDefault(),
            LastLoggedInAtUtc: lastLoggedInAtUtc,
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
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
        {
            return false;
        }

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
    }
}