using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Stigar.Core.Logging;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.StravaClient;


namespace Stigar.Api.Endpoints.Backend
{
    public class RefreshStravaToken(AuthenticationApi _authApi, IUserRepository _userRepository, ILogger<RefreshStravaToken> _logger)
    {
        public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
            app.MapGet("/api/{userId}/accessToken", (HttpContext context, [FromServices] RefreshStravaToken endpoint, string userId) => endpoint.Handle(context, userId, context.RequestAborted));

        public async Task<IResult> Handle(HttpContext context, string userId, CancellationToken cancellationToken)
        {
            var safeUserId = LogInput.Sanitize(userId);
            var user = await _userRepository.GetById(userId);

            if (user == default || user.RefreshToken == null)
            {
                return TypedResults.NotFound($"Refresh token not found for user {safeUserId}");
            }

            if (user.AccessToken != null && user.TokenExpiresAt > new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds())
            {
                return TypedResults.Ok(user.AccessToken);
            }

            var tokenResponse = default(Stigar.Core.Services.StravaClient.Model.TokenResponse);
            try
            {
                tokenResponse = await _authApi.RefreshToken(user.RefreshToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to refresh Strava token for user {UserId}", safeUserId);
                throw;
            }

            user.AccessToken = tokenResponse.AccessToken;
            user.TokenExpiresAt = tokenResponse.ExpiresAt;
            user.StravaScope = tokenResponse.Scope ?? user.StravaScope;
            await _userRepository.Upsert(user);

            return TypedResults.Ok(tokenResponse.AccessToken);
        }
    }
}
