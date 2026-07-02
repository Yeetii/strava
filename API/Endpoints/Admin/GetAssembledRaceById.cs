using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public class GetAssembledRaceById(
    BlobOrganizerStore organizerStore,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"], Summary = "Inspect one assembled race feature.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "featureId", In = ParameterLocation.Path, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(StoredFeature))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NotFound)]
    [Function(nameof(GetAssembledRaceById))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/races/assembled/{featureId}")] HttpRequestData req,
        string featureId,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var organizerKey = featureId.Contains('-', StringComparison.Ordinal)
            ? featureId[..featureId.LastIndexOf('-')]
            : featureId;
        var feature = await organizerStore.GetAssembledRaceAsync(organizerKey, featureId, cancellationToken);
        if (feature is null)
        {
            return req.CreateResponse(HttpStatusCode.NotFound);
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(feature, cancellationToken);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey)) return false;
        return req.Headers.TryGetValues("x-admin-key", out var provided) && provided.FirstOrDefault() == adminKey;
    }
}
