using System.Net;
using Shared.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;

namespace API
{
    public class TokenBody
    {
        public string? RefreshToken { get; set; }
        public string? AuthorizationToken { get; set; }
    }


    public class PostLogin(ILoggerFactory loggerFactory)
    {
        private readonly ILogger _logger = loggerFactory.CreateLogger<PostLogin>();

        [OpenApiOperation(tags: ["User management"])]
        [OpenApiParameter(name: "userId", In = ParameterLocation.Path)]
        [OpenApiRequestBody(contentType: "application/json", bodyType: typeof(TokenBody), Description = "Must provide either a refresh token or an authorization token")]
        [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(string), Description = "The OK response")]
        [Function(nameof(PostLogin))]
        public async Task<ReturnType> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = "{userId}/login")] HttpRequestData req, string userId)
        {
            var body = await req.ReadFromJsonAsync<TokenBody>();

            var refreshToken = body.RefreshToken;

            if (refreshToken == null)
            {
                if (body.AuthorizationToken == null){
                    var resp = req.CreateResponse(HttpStatusCode.BadRequest);
                    await resp.WriteStringAsync("No tokens provided");
                    return new ReturnType{Result = resp};
                }
                
                // TODO: Get refresh token from auth token
                refreshToken = "Something non null";
            }      

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
                SameSite = SameSite.Lax,
                Path = "/"
            };
            response.Cookies.Append(cookie);
            await response.WriteStringAsync("Added user");

            var user = new User{Id = userId, 
                RefreshToken = refreshToken, 
                SessionId = sessionId, 
                SessionExpires = expirationDate};

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
