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

public record AdminUserStatus(
    string UserId,
    string? Username,
    string? FirstName,
    string? LastName,
    int ActivityCount,
    DateTime? LastLoggedInAtUtc);

internal sealed record UserSummaryProjection(
    string Id,
    string? UserName,
    string? FirstName,
    string? LastName);

internal sealed record ActivityCountProjection(
    string UserId,
    int ActivityCount);

internal sealed record AdminUsersLastLoginProjection(
    string UserId,
    DateTime? LastLoggedInAtUtc);

public class GetUsersStatus(
    CollectionClient<UserDocument> usersCollection,
    CollectionClient<Activity> activitiesCollection,
    CollectionClient<Session> sessionsCollection,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<AdminUserStatus>))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetUsersStatus))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/users")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
        {
            return req.CreateResponse(HttpStatusCode.Unauthorized);
        }

        var usersTask = usersCollection.ExecuteQueryAsync<UserSummaryProjection>(
            new QueryDefinition("SELECT c.id, c.userName, c.firstName, c.lastName FROM c"));
        var activityCountsTask = activitiesCollection.ExecuteQueryAsync<ActivityCountProjection>(
            new QueryDefinition("SELECT c.userId AS userId, COUNT(1) AS activityCount FROM c GROUP BY c.userId"));
        var lastLoginTask = sessionsCollection.ExecuteQueryAsync<AdminUsersLastLoginProjection>(
            new QueryDefinition("SELECT c.userId AS userId, MAX(c.createdAtUtc) AS lastLoggedInAtUtc FROM c WHERE IS_DEFINED(c.createdAtUtc) GROUP BY c.userId"));

        await Task.WhenAll(usersTask, activityCountsTask, lastLoginTask);

        var activityCountsByUserId = activityCountsTask.Result.ToDictionary(item => item.UserId, item => item.ActivityCount, StringComparer.OrdinalIgnoreCase);
        var lastLoginByUserId = lastLoginTask.Result.ToDictionary(item => item.UserId, item => item.LastLoggedInAtUtc, StringComparer.OrdinalIgnoreCase);

        var users = usersTask.Result
            .Select(user => new AdminUserStatus(
                UserId: user.Id,
                Username: user.UserName,
                FirstName: user.FirstName,
                LastName: user.LastName,
                ActivityCount: activityCountsByUserId.TryGetValue(user.Id, out var activityCount) ? activityCount : 0,
                LastLoggedInAtUtc: lastLoginByUserId.TryGetValue(user.Id, out var lastLoggedInAtUtc) ? lastLoggedInAtUtc : null))
            .OrderByDescending(user => user.LastLoggedInAtUtc ?? DateTime.MinValue)
            .ThenByDescending(user => user.ActivityCount)
            .ThenBy(user => user.Username ?? user.UserId)
            .ToArray();

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(users);
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