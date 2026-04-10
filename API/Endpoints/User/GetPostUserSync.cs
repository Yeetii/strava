using System.Net;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;
using API.Utils;

namespace API.Endpoints.User;

public class GetUserSync(UserAuthenticationService userAuthService, UserSyncService userSyncService)
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    [OpenApiOperation(tags: ["User management"])]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "CORS preflight response")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(UserSyncPayload),
        Description = "The authenticated user's synced Trailscope files and settings.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.BadRequest, contentType: "text/plain", bodyType: typeof(string),
        Description = "Invalid sync payload.")]
    [Function(nameof(GetUserSync))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", "options", Route = "userSync")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (CorsHeaders.IsOptions(req))
        {
            var optionsResponse = req.CreateResponse(HttpStatusCode.NoContent);
            CorsHeaders.Add(req, optionsResponse, "GET, POST, OPTIONS");
            return optionsResponse;
        }

        var response = req.CreateResponse();
        CorsHeaders.Add(req, response, "GET, POST, OPTIONS");

        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
        {
            response.StatusCode = HttpStatusCode.Unauthorized;
            return response;
        }

        try
        {
            if (req.Method.Equals("GET", StringComparison.OrdinalIgnoreCase))
            {
                var snapshot = await userSyncService.GetSnapshot(user.Id, cancellationToken);
                response.StatusCode = HttpStatusCode.OK;
                await response.WriteAsJsonAsync(snapshot, cancellationToken);
                return response;
            }

            if (req.Method.Equals("POST", StringComparison.OrdinalIgnoreCase))
            {
                var payload = await JsonSerializer.DeserializeAsync<UserSyncPayload>(req.Body, SerializerOptions, cancellationToken);
                if (payload == null)
                {
                    response.StatusCode = HttpStatusCode.BadRequest;
                    await response.WriteStringAsync("Invalid sync payload.", cancellationToken);
                    return response;
                }

                var mergedPayload = await userSyncService.ApplyChanges(user.Id, payload, cancellationToken);
                response.StatusCode = HttpStatusCode.OK;
                await response.WriteAsJsonAsync(mergedPayload, cancellationToken);
                return response;
            }

            response.StatusCode = HttpStatusCode.MethodNotAllowed;
            return response;
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            return RequestCancellation.CreateCancelledResponse(req);
        }
    }
}