using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.StravaClient;
using UserDocument = Shared.Models.User;
using API.Http;

namespace API.Endpoints.Admin;

public record RefreshUserStravaTokenResponse(string UserId, AdminStravaAuthStatus StravaAuthStatus);

public class RefreshUserStravaToken(
    IUserRepository usersRepository,
    AuthenticationApi authenticationApi,
    IConfiguration configuration)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/users/{userId}/strava/refreshToken",
            (HttpContext context, [FromServices] RefreshUserStravaToken endpoint, string userId) => endpoint.Handle(context, userId, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string userId, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var user = await usersRepository.GetById(userId, cancellationToken);
        if (user == null)
            return TypedResults.NotFound();

        if (string.IsNullOrWhiteSpace(user.RefreshToken))
            return TypedResults.BadRequest($"Refresh token not found for user {userId}");

        var tokenResponse = await authenticationApi.RefreshToken(user.RefreshToken);
        user.AccessToken = tokenResponse.AccessToken;
        user.TokenExpiresAt = tokenResponse.ExpiresAt;
        user.RefreshToken = tokenResponse.RefreshToken;
        user.StravaScope = tokenResponse.Scope ?? user.StravaScope;

        await usersRepository.Upsert(user, cancellationToken);

        return TypedResults.Ok(
            new RefreshUserStravaTokenResponse(user.Id, GetUserStats.BuildStravaAuthStatus(user)));
    }
}
