using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using Shared.Services.StravaClient;


namespace Backend
{
    public class RefreshStravaToken(AuthenticationApi _authApi)
    {

        [Function("RefreshStravaToken")]
        public async Task<ReturnType> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "{userId}/accessToken")] HttpRequestData req, string userId,
            [CosmosDBInput(
            databaseName: "%CosmosDb%",
            containerName: "%UsersContainer%",
            Connection  = "CosmosDBConnection",
            Id = "{userId}",
            PartitionKey = "{userId}")] User user)
        {
            if (user.AccessToken != null && user.TokenExpiresAt > new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds())
            {
                var tokenStillWorksResponse = req.CreateResponse(System.Net.HttpStatusCode.OK);
                await tokenStillWorksResponse.WriteStringAsync(user.AccessToken);
                return new ReturnType{Result = tokenStillWorksResponse};
            }

            var refreshToken = user.RefreshToken;

            if (refreshToken == null)
            {
                var notFoundResult = req.CreateResponse(System.Net.HttpStatusCode.NotFound);
                await notFoundResult.WriteStringAsync($"Refresh token not found for user {userId}");
                return new ReturnType{Result = notFoundResult};
            }
            var tokenResponse = await _authApi.RefreshToken(refreshToken);

            user.AccessToken = tokenResponse.AccessToken;
            user.TokenExpiresAt = tokenResponse.ExpiresAt;

            var result = req.CreateResponse(System.Net.HttpStatusCode.OK);
            await result.WriteStringAsync(tokenResponse.AccessToken);

            return new ReturnType{Result = result, WriteToUser = user};
        }

        public class ReturnType
        {
            [HttpResult]
            public required HttpResponseData Result { get; set;}
            [CosmosDBOutput("%CosmosDb%", "%UsersContainer%", Connection = "CosmosDBConnection", CreateIfNotExists = true, PartitionKey = "/id")]
            public User? WriteToUser { get; set;}
        }
    }
}