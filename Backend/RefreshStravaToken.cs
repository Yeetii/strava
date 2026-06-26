using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;


namespace Backend
{
    public class RefreshStravaToken(AuthenticationApi _authApi, CollectionClient<User> _userCollection, ILogger<RefreshStravaToken> _logger)
    {

        [Function("RefreshStravaToken")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "{userId}/accessToken")] HttpRequestData req, string userId)
        {
            var user = await _userCollection.GetByIdMaybe(userId, new Microsoft.Azure.Cosmos.PartitionKey(userId));

            if (user == default || user.RefreshToken == null)
            {
                var notFoundResult = req.CreateResponse(System.Net.HttpStatusCode.NotFound);
                await notFoundResult.WriteStringAsync($"Refresh token not found for user {userId}");
                return notFoundResult;
            }

            if (user.AccessToken != null && user.TokenExpiresAt > new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds())
            {
                var tokenStillWorksResponse = req.CreateResponse(System.Net.HttpStatusCode.OK);
                await tokenStillWorksResponse.WriteStringAsync(user.AccessToken);
                return tokenStillWorksResponse;
            }

            var tokenResponse = default(Shared.Services.StravaClient.Model.TokenResponse);
            try
            {
                tokenResponse = await _authApi.RefreshToken(user.RefreshToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to refresh Strava token for user {UserId}", userId);
                throw;
            }

            user.AccessToken = tokenResponse.AccessToken;
            user.TokenExpiresAt = tokenResponse.ExpiresAt;
            user.StravaScope = tokenResponse.Scope ?? user.StravaScope;
            await _userCollection.UpsertDocument(user);

            var result = req.CreateResponse(System.Net.HttpStatusCode.OK);
            await result.WriteStringAsync(tokenResponse.AccessToken);

            return result;
        }
    }
}
