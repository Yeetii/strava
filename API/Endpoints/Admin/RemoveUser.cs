using Microsoft.AspNetCore.Mvc;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Shared.Jobs;

namespace API.Endpoints.Admin;

public class RemoveUser(IBackgroundJobClient jobs)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapDelete("/api/manage/users/{userId}",
            (HttpContext context, [FromServices] RemoveUser endpoint, string userId) => endpoint.Handle(context, userId));

    public IResult Handle(HttpContext context, string userId)
    {
        jobs.EnqueueAccountDelete(new AccountDeleteJob { UserId = userId });
        return TypedResults.NoContent();
    }
}
