using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Stigar.Core.Models;
using Stigar.Core.Services.Postgres;
using UserDocument = Stigar.Core.Models.User;
using Stigar.Api.Http;

namespace Stigar.Api.Endpoints.Admin;

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
    IUserSyncItemRepository syncItemsRepository,
    IConfiguration configuration)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/users",
            (HttpContext context, [FromServices] GetUsersStatus endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var usersTask = usersRepository.GetAll();
        var countsTask = activitiesRepository.GetActivityCountsByUser();
        var syncTask = syncItemsRepository.GetLatestUpdatedAtByUsers();

        await Task.WhenAll(usersTask, countsTask, syncTask);

        var users = await usersTask;
        var activityCounts = await countsTask;
        var lastSyncs = await syncTask;

        var result = users
            .Select(user => new AdminUserStatus(
                UserId: user.Id,
                Username: user.UserName,
                FirstName: user.FirstName,
                LastName: user.LastName,
                ActivityCount: activityCounts.GetValueOrDefault(user.Id, 0),
                LastLoggedInAtUtc: UserSyncItem.ToUtcDateTime(
                    lastSyncs.TryGetValue(user.Id, out var updatedAt) ? updatedAt : null)))
            .OrderByDescending(user => user.LastLoggedInAtUtc ?? DateTime.MinValue)
            .ThenByDescending(user => user.ActivityCount)
            .ThenBy(user => user.Username ?? user.UserId)
            .ToArray();

        return TypedResults.Ok(result);
    }
}
