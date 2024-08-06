using System.Net;
using API.Models;
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


    public class AddUser
    {
        private readonly ILogger _logger;

        public AddUser(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<AddUser>();
        }

        [OpenApiOperation(tags: ["User management"])]
        [OpenApiParameter(name: "userId", In = ParameterLocation.Path)]
        [OpenApiRequestBody(contentType: "application/json", bodyType: typeof(TokenBody), Description = "Must provide either a refresh token or an authorization token")]
        [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(string), Description = "The OK response")]
        [Function("AddUser")]
        public async Task<ReturnType> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = "{userId}/addUser")] HttpRequestData req, string userId)
        {
            var body = await req.ReadFromJsonAsync<TokenBody>();

            var refreshToken = body.RefreshToken;

            if (refreshToken == null)
            {
                if (body.AuthorizationToken == null)
                    return new ReturnType{Result = Results.BadRequest("No tokens provided")};
                
                // TODO: Get refresh token from auth token
                refreshToken = "Something non null";
            }

            if (refreshToken == null)
                return new ReturnType{Result = Results.BadRequest("Auth token exchange did not provide refresh token")};

            return new ReturnType{Result = Results.Ok("Stored user in database"), User = new User{Id = userId, RefreshToken = refreshToken}};
        }

        public class ReturnType
        {
            [HttpResult]
            public IResult Result { get; set;}
            [CosmosDBOutput("%CosmosDb%", "%UsersContainer%", Connection = "CosmosDBConnection", CreateIfNotExists = true, PartitionKey = "/id")]
            public User? User { get; set;}
        }
    }
}
