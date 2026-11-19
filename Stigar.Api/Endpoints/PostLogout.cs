using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;

namespace Stigar.Api.Endpoints;

public class PostLogout
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/logout", (HttpContext context, [FromServices] PostLogout endpoint) => endpoint.Handle(context));

    public IResult Handle(HttpContext context)
    {
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
