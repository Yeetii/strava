using System.Net;
using Shared.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services.StravaClient;
using Shared.Services;

namespace API.Endpoints
{
    public class PostLogin(AuthenticationApi _authenticationApi, CollectionClient<User> _usersCollection)
    {
        [OpenApiOperation(tags: ["User management"])]
        [OpenApiParameter(name: "authCode", In = ParameterLocation.Path)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(string), Description = "The OK response")]
        [Function(nameof(PostLogin))]
        public async Task<ReturnBindings> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "{authCode}/login")] HttpRequestData req, string authCode)
        {
            var isLocal = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_FUNCTIONS_ENVIRONMENT")); // set to "Development" locally

            var response = req.CreateResponse();
            response.Headers.Add("Access-Control-Allow-Credentials", "true");
            var outputs = new ReturnBindings() { Response = response };

            if (string.IsNullOrEmpty(authCode))
            {
                response.StatusCode = HttpStatusCode.BadRequest;
                await response.WriteStringAsync("No tokens provided");
                return outputs;
            }

            var tokenResponse = await _authenticationApi.TokenExcange(authCode);
            var refreshToken = tokenResponse.RefreshToken;

            if (refreshToken == null)
            {
                response.StatusCode = HttpStatusCode.BadRequest;
                await response.WriteStringAsync("Auth token exchange did not provide refresh token");
                return outputs;
            }

            response.StatusCode = HttpStatusCode.OK;
            var expirationDate = DateTime.Now.AddDays(30);
            var sessionId = Guid.NewGuid();
            var cookie = new HttpCookie("session", sessionId.ToString())
            {
                Expires = expirationDate,
                SameSite = SameSite.ExplicitNone,
                Secure = true,
                Domain = isLocal ? "localhost" : "erikmagnusson.com",
                Path = "/"
            };
            response.Cookies.Append(cookie);
            await response.WriteStringAsync("Added user");

            var userId = tokenResponse.Athlete.Id.ToString();

            var userExist = await _usersCollection.GetByIdMaybe(userId, new Microsoft.Azure.Cosmos.PartitionKey(userId));

            if (userExist == null)
            {
                var fetchJob = new ActivitiesFetchJob { UserId = userId };
                outputs.ActivitiesFetchJob = fetchJob;
            }

            outputs.User = new User
            {
                Id = tokenResponse.Athlete.Id.ToString(),
                UserName = tokenResponse.Athlete.Username,
                RefreshToken = refreshToken,
                SessionId = sessionId,
                SessionExpires = expirationDate,
                AccessToken = tokenResponse.AccessToken,
                TokenExpiresAt = tokenResponse.ExpiresAt
            };

            return outputs;
        }

        public class ReturnBindings
        {
            [HttpResult]
            public required HttpResponseData Response { get; set; }
            [CosmosDBOutput("%CosmosDb%", "%UsersContainer%", Connection = "CosmosDBConnection", CreateIfNotExists = true, PartitionKey = "/id")]
            public User? User { get; set; }
            [ServiceBusOutput("activitiesfetchjobs", Connection = "ServicebusConnection")]
            public ActivitiesFetchJob? ActivitiesFetchJob { get; set; }
        }
    }
}
