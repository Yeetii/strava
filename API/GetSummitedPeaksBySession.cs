using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API
{
    public class GetSummitedPeaksBySession(CollectionClient<User> _usersCollection, CollectionClient<SummitedPeak> _summitedPeakCollection)
    {
        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiParameter(name: "session", In = ParameterLocation.Cookie)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<SummitedPeak>), 
            Description = "Peaks that the user has summited")]
        [Function(nameof(GetSummitedPeaksBySession))]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "summitedPeaks")] HttpRequestData req)
        {
            string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;

            Thread.Sleep(1500);

            var response = req.CreateResponse();
            // Probably won't need this in production if I move the API to the same domain as the frontend
            response.Headers.Add("Access-Control-Allow-Credentials", "true");

            if (sessionId == default){
                response.StatusCode = HttpStatusCode.Unauthorized;
                return response;
            }
            var user = (await _usersCollection.QueryCollection($"SELECT * from c WHERE c.sessionId = '{sessionId}'")).FirstOrDefault();
            if (user == default || user.SessionExpires < DateTime.Now){
                response.StatusCode = HttpStatusCode.Unauthorized;
                return response;
            }

            var peaks = await _summitedPeakCollection.QueryCollection($"SELECT * FROM c where c.userId = '{user.Id}'");
            response.StatusCode = HttpStatusCode.OK;
            await response.WriteAsJsonAsync(peaks);
            return response;
        }
    }
}
