using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using API.Utils;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.User;

public class DeleteAccount(UserAuthenticationService _userAuthService)
{
    [OpenApiOperation(tags: ["User management"], Summary = "Submit an account delete job for the current user.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "CORS preflight response")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "Account delete job queued")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [Function(nameof(DeleteAccount))]
    public async Task<ReturnBindings> Run([HttpTrigger(AuthorizationLevel.Anonymous, "delete", "options", Route = "account")] HttpRequestData req)
    {
        var response = req.CreateResponse();
        CorsHeaders.Add(req, response, "DELETE, OPTIONS");
        var outputs = new ReturnBindings { Response = response };

        if (CorsHeaders.IsOptions(req))
        {
            response.StatusCode = HttpStatusCode.NoContent;
            return outputs;
        }

        var sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await _userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
        {
            response.StatusCode = HttpStatusCode.Unauthorized;
            await response.WriteStringAsync("Invalid session");
            return outputs;
        }

        outputs.AccountDeleteJob = new AccountDeleteJob
        {
            UserId = user.Id
        };

        var isLocal = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_FUNCTIONS_ENVIRONMENT"));
        var cookie = new HttpCookie("session", string.Empty)
        {
            MaxAge = 0,
            SameSite = SameSite.ExplicitNone,
            Secure = true,
            Domain = isLocal ? "localhost" : "erikmagnusson.com",
            Path = "/"
        };
        response.Cookies.Append(cookie);

        response.StatusCode = HttpStatusCode.NoContent;
        return outputs;
    }

    public class ReturnBindings
    {
        [HttpResult]
        public required HttpResponseData Response { get; set; }

        [ServiceBusOutput(ServiceBusConfig.AccountDeleteJobs, Connection = "ServicebusConnection")]
        public AccountDeleteJob? AccountDeleteJob { get; set; }
    }
}
