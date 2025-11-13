using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Extensions;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.User;

public class GetActivities(UserAuthenticationService _userAuthService, CollectionClient<Activity> _activitiesCollectionClient)
{
    [OpenApiOperation(tags: ["Activities"])]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection))]
    [Function("GetActivities")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "activities")] HttpRequestData req)
    {
        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await _userAuthService.GetUserFromSessionId(sessionId);
        if (user == default)
        {
            return req.CreateResponse(HttpStatusCode.Unauthorized);
        }

        var activitiesQuery = new QueryDefinition($"SELECT * FROM c where c.userId = '{user.Id}'");
        var activities = (await _activitiesCollectionClient.ExecuteQueryAsync<Activity>(activitiesQuery)).ToList();

        var features = activities.Where(a => !a.SummaryPolyline.IsNullOrWhiteSpace()).Select(a => a.ToFeature());
        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new FeatureCollection(features));
        return response;
    }
}