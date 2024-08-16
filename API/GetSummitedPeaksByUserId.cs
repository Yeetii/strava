using System.Net;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;

namespace API
{
    public static class GetSummitedPeaksByUserId
    {
        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiParameter(name: "userId", In = ParameterLocation.Path)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<SummitedPeak>), 
            Description = "Peaks that the user has summited")]
        [Function(nameof(GetSummitedPeaksByUserId))]
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
