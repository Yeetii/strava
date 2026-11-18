using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Services.Postgres.Admin;

namespace API.Endpoints.Admin;

public class GetCorruptedAdminBoundaries(
    IOsmFeatureAdminQueries adminQueries,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(string[]),
        Description = "IDs of admin_boundaries rows where the name JSON value is not a string.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetCorruptedAdminBoundaries))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/adminBoundaries/corrupted")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var ids = await adminQueries.GetCorruptedAdminBoundaryIdsAsync();

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(ids);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
            return false;

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
    }
}
