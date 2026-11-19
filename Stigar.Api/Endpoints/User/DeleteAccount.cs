using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Stigar.Core.Jobs;
using Stigar.Core.Services;

namespace Stigar.Api.Endpoints.User;

public class DeleteAccount(UserAuthenticationService _userAuthService, IBackgroundJobClient jobs)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapMethods("/api/account", ["DELETE", "OPTIONS"],
            (HttpContext context, [FromServices] DeleteAccount endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (context.IsOptionsRequest())
            return TypedResults.NoContent();

        var user = await _userAuthService.GetUserFromSessionId(context.SessionId());
        if (user == null)
            return TypedResults.Unauthorized();

        jobs.EnqueueAccountDelete(new AccountDeleteJob { UserId = user.Id });

        context.Response.Cookies.Delete("session", new CookieOptions
        {
            Secure = true,
            HttpOnly = false,
            SameSite = SameSiteMode.None,
            Path = "/",
        });
        return TypedResults.NoContent();
    }
}
