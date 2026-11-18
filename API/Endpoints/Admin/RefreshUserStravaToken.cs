using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.StravaClient;
using UserDocument = Shared.Models.User;

namespace API.Endpoints.Admin;

public record RefreshUserStravaTokenResponse(string UserId, AdminStravaAuthStatus StravaAuthStatus);

public class RefreshUserStravaToken(
    IUserRepository usersRepository,
    AuthenticationApi authenticationApi,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"], Summary = "Refresh a user's Strava token and persist returned auth metadata.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "userId", In = ParameterLocation.Path, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(RefreshUserStravaTokenResponse))]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.BadRequest, contentType: "text/plain", bodyType: typeof(string))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NotFound)]
    [Function(nameof(RefreshUserStravaToken))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/users/{userId}/strava/refreshToken")] HttpRequestData req,
        string userId,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
        {
            return req.CreateResponse(HttpStatusCode.Unauthorized);
        }

        var user = await usersRepository.GetById(userId, cancellationToken);
        if (user == null)
        {
            return req.CreateResponse(HttpStatusCode.NotFound);
        }

        if (string.IsNullOrWhiteSpace(user.RefreshToken))
        {
            var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
            await badRequest.WriteStringAsync($"Refresh token not found for user {userId}", cancellationToken);
            return badRequest;
        }

        var tokenResponse = await authenticationApi.RefreshToken(user.RefreshToken);
        user.AccessToken = tokenResponse.AccessToken;
        user.TokenExpiresAt = tokenResponse.ExpiresAt;
        user.RefreshToken = tokenResponse.RefreshToken;
        user.StravaScope = tokenResponse.Scope ?? user.StravaScope;

        await usersRepository.Upsert(user, cancellationToken);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(
            new RefreshUserStravaTokenResponse(user.Id, GetUserStats.BuildStravaAuthStatus(user)),
            cancellationToken);
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
