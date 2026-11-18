using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services.Postgres;
using UserDocument = Shared.Models.User;

namespace API.Endpoints.Admin;

public record AdminUserStatus(
    string UserId,
    string? Username,
    string? FirstName,
    string? LastName,
    int ActivityCount,
    DateTime? LastLoggedInAtUtc);

public class GetUsersStatus(
    IUserRepository usersRepository,
    IActivityRepository activitiesRepository,
    ISessionRepository sessionsRepository,
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

        var users = await usersRepository.GetAll();
        var usersWithCounts = await Task.WhenAll(users.Select(async user => new
        {
            User = user,
            ActivityCount = (await activitiesRepository.GetAllByUser(user.Id)).Count,
            LastLoggedInAtUtc = (await sessionsRepository.GetByUserId(user.Id))
                .Select(session => (DateTime?)session.CreatedAtUtc)
                .Max(),
        }));

        var result = usersWithCounts
            .Select(item => new AdminUserStatus(
                UserId: item.User.Id,
                Username: item.User.UserName,
                FirstName: item.User.FirstName,
                LastName: item.User.LastName,
                ActivityCount: item.ActivityCount,
                LastLoggedInAtUtc: item.LastLoggedInAtUtc))
            .OrderByDescending(user => user.LastLoggedInAtUtc ?? DateTime.MinValue)
            .ThenByDescending(user => user.ActivityCount)
            .ThenBy(user => user.Username ?? user.UserId)
            .ToArray();

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(result);
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
