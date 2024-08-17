using System.Net;
using Shared.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services.StravaClient;

namespace API
{
    public class PostLogin(AuthenticationApi _authenticationApi)
    {
        [OpenApiOperation(tags: ["User management"])]
        [OpenApiParameter(name: "authCode", In = ParameterLocation.Path)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(string), Description = "The OK response")]
        [Function(nameof(PostLogin))]
        public async Task<ReturnType> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "{authCode}/login")] HttpRequestData req, string authCode)
        {
            if (string.IsNullOrEmpty(authCode)){
                var resp = req.CreateResponse(HttpStatusCode.BadRequest);
                await resp.WriteStringAsync("No tokens provided");
                return new ReturnType{Result = resp};
            }
            
            var tokenResponse = await _authenticationApi.TokenExcange(authCode);
            var refreshToken = tokenResponse.RefreshToken;
                

            if (refreshToken == null){
                var resp = req.CreateResponse(HttpStatusCode.BadRequest);
                await resp.WriteStringAsync("Auth token exchange did not provide refresh token");
                return new ReturnType{Result = resp};
            }

            var response = req.CreateResponse(HttpStatusCode.OK);
            var expirationDate = DateTime.Now.AddDays(30);
            var sessionId = Guid.NewGuid();
            var cookie = new HttpCookie("session", sessionId.ToString())
            {
                Expires = expirationDate,
                SameSite = SameSite.ExplicitNone,
                Secure = true,
                Path = "/"
            };
            response.Cookies.Append(cookie);
            response.Headers.Add("Access-Control-Allow-Credentials", "true");
            await response.WriteStringAsync("Added user");

            var user = new User{Id = tokenResponse.Athlete.Id.ToString(), 
                RefreshToken = refreshToken, 
                SessionId = sessionId, 
                SessionExpires = expirationDate,
                AccessToken = tokenResponse.AccessToken,
                TokenExpiresAt = tokenResponse.ExpiresAt};

            return new ReturnType{Result = response, User = user};
        }

        public class ReturnType
        {
            [HttpResult]
            public HttpResponseData Result { get; set;}
            [CosmosDBOutput("%CosmosDb%", "%UsersContainer%", Connection = "CosmosDBConnection", CreateIfNotExists = true, PartitionKey = "/id")]
            public User? User { get; set;}
        }
    }
}
