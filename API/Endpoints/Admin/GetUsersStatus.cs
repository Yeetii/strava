using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Shared.Models;
using Shared.Services.Postgres;
using UserDocument = Shared.Models.User;
using API.Http;

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
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/users",
            (HttpContext context, [FromServices] GetUsersStatus endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var usersTask = usersRepository.GetAll();
        var countsTask = activitiesRepository.GetActivityCountsByUser();
        var loginsTask = sessionsRepository.GetLastLoginByUsers();

        await Task.WhenAll(usersTask, countsTask, loginsTask);

        var users = await usersTask;
        var activityCounts = await countsTask;
        var lastLogins = await loginsTask;

        var result = users
            .Select(user => new AdminUserStatus(
                UserId: user.Id,
                Username: user.UserName,
                FirstName: user.FirstName,
                LastName: user.LastName,
                ActivityCount: activityCounts.GetValueOrDefault(user.Id, 0),
                LastLoggedInAtUtc: lastLogins.TryGetValue(user.Id, out var lastLogin) ? lastLogin : null))
            .OrderByDescending(user => user.LastLoggedInAtUtc ?? DateTime.MinValue)
            .ThenByDescending(user => user.ActivityCount)
            .ThenBy(user => user.Username ?? user.UserId)
            .ToArray();

        return TypedResults.Ok(result);
    }
}
