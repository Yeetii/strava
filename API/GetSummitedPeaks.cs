using System.Net;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.OpenApi.Models;
using Shared.Models;

namespace API
{
    public class GetSummitedPeaks()
    {
        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
        [OpenApiParameter(name: "userId", In = ParameterLocation.Path)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<SummitedPeak>), 
            Description = "Peaks that the user has summited")]
        [Function("GetSummitedPeaks")]
        public static IActionResult Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "{userId}/summitedPeaks")] HttpRequestData req, string userId,
            [CosmosDBInput(
            databaseName: "%CosmosDb%",
            containerName: "%SummitedPeaksContainer%",
            Connection  = "CosmosDBConnection",
            SqlQuery = "SELECT * FROM c where c.userId = {userId}",
            PartitionKey = "{userId}"
            )] IEnumerable<SummitedPeak> peaks
        )
        {
            return new JsonResult(peaks);
        }
    }
}
