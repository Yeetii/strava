using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public class FetchCountries(
    AdminBoundariesCollectionClient adminBoundariesCollectionClient,
    IConfiguration configuration,
    ILogger<FetchCountries> logger)
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "countryCodes", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Comma-separated ISO 3166-1 alpha-2 country codes to fetch. When omitted, all missing countries are fetched.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json",
        bodyType: typeof(AdminBoundariesCollectionClient.CountryFetchResult))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(FetchCountries))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/countries/fetch")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var codesParam = req.Query["countryCodes"];
        var countryCodes = string.IsNullOrWhiteSpace(codesParam)
            ? null
            : codesParam.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(c => c.ToUpperInvariant())
                .ToList();

        var result = await adminBoundariesCollectionClient.FetchAndStoreCountries(countryCodes, logger, cancellationToken);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(result);
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
