using Microsoft.AspNetCore.Mvc;
using Hangfire;
using Shared.Jobs;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using API.Http;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using UserDocument = Shared.Models.User;

namespace API.Endpoints.Admin;

public record RefetchUserActivitiesResponse(string UserId, bool Queued);

public class RefetchUserActivities(
    IUserRepository usersRepository,
    IBackgroundJobClient jobs,
    IConfiguration configuration)
{

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/users/{userId}/activities/refetch",
            (HttpContext context, [FromServices] RefetchUserActivities endpoint, string userId) => endpoint.Handle(context, userId, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string userId, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
        {
            return TypedResults.Unauthorized();
        }

        var user = await usersRepository.GetById(userId, cancellationToken);
        if (user == null)
        {
            return TypedResults.NotFound();
        }

        var fetchJob = new ActivitiesFetchJob { UserId = userId, Page = 1 };
        jobs.EnqueueActivitiesFetch(fetchJob);

        return TypedResults.Ok(new RefetchUserActivitiesResponse(userId, Queued: true));
    }
}
