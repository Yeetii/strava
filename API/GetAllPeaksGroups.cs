using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;

namespace API
{
    public class GetAllPeaksGroups()
    {
        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<PeaksGroup>))]
        [Function(nameof(GetAllPeaksGroups))]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "peaksGroups")] HttpRequestData req,
        [CosmosDBInput(
            databaseName: "%OsmDb%",
            containerName: "%PeaksGroupsContainer%",
            Connection  = "CosmosDBConnection",
            SqlQuery = "SELECT * FROM c"
            )] IEnumerable<PeaksGroup> groups)
        {
            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(groups);
            return response;
        }
    }
}
