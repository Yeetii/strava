using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;


namespace Backend
{
    public class RefreshStravaToken(AuthenticationApi _authApi, CollectionClient<User> _userCollection)
    {

        [Function("RefreshStravaToken")]
        public async Task<ReturnType> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "{userId}/accessToken")] HttpRequestData req, string userId)
        {
            var user = await _userCollection.GetByIdMaybe(userId, new Microsoft.Azure.Cosmos.PartitionKey(userId));

            if (user == default || user.RefreshToken == null)
            {
                var notFoundResult = req.CreateResponse(System.Net.HttpStatusCode.NotFound);
                await notFoundResult.WriteStringAsync($"Refresh token not found for user {userId}");
                return new ReturnType { Result = notFoundResult };
            }

            if (user.AccessToken != null && user.TokenExpiresAt > new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds())
            {
                var tokenStillWorksResponse = req.CreateResponse(System.Net.HttpStatusCode.OK);
                await tokenStillWorksResponse.WriteStringAsync(user.AccessToken);
                return new ReturnType { Result = tokenStillWorksResponse };
            }

            var tokenResponse = await _authApi.RefreshToken(user.RefreshToken);

            user.AccessToken = tokenResponse.AccessToken;
            user.TokenExpiresAt = tokenResponse.ExpiresAt;

            var result = req.CreateResponse(System.Net.HttpStatusCode.OK);
            await result.WriteStringAsync(tokenResponse.AccessToken);

            return new ReturnType { Result = result, WriteToUser = user };
        }

        public class ReturnType
        {
            [HttpResult]
            public required HttpResponseData Result { get; set; }
            [CosmosDBOutput("%CosmosDb%", "%UsersContainer%", Connection = "CosmosDBConnection", CreateIfNotExists = true, PartitionKey = "/id")]
            public User? WriteToUser { get; set; }
        }
    }
}