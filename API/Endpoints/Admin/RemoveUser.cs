using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Models;

namespace API.Endpoints.Admin;

public class RemoveUser
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "userId", In = ParameterLocation.Path)]
    [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "Account delete job queued")]
    [Function(nameof(RemoveUser))]
    public ReturnBindings Run(
        [HttpTrigger(AuthorizationLevel.Function, "delete", Route = "manage/users/{userId}")] HttpRequestData req,
        string userId)
    {
        return new ReturnBindings
        {
            Response = req.CreateResponse(HttpStatusCode.NoContent),
            AccountDeleteJob = new AccountDeleteJob { UserId = userId }
        };
    }

    public class ReturnBindings
    {
        [HttpResult]
        public required HttpResponseData Response { get; set; }

        [ServiceBusOutput(ServiceBusConfig.AccountDeleteJobs, Connection = "ServiceBusConnection")]
        public AccountDeleteJob? AccountDeleteJob { get; set; }
    }
}
