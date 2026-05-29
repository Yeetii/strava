using System.Net;
using API.Utils;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;

namespace API.Endpoints.OpenApi;

public class GetSourceOpenApiDocument
{
    private static readonly string SourceDocumentPath = Path.Combine(AppContext.BaseDirectory, "openapi", "openapi.source.json");

    [Function(nameof(GetSourceOpenApiDocument))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", "options", Route = "openapi/source.json")]
        HttpRequestData req)
    {
        var response = req.CreateResponse();
        CorsHeaders.Add(req, response, "GET, OPTIONS");

        if (CorsHeaders.IsOptions(req))
        {
            response.StatusCode = HttpStatusCode.NoContent;
            return response;
        }

        if (!File.Exists(SourceDocumentPath))
        {
            response.StatusCode = HttpStatusCode.NotFound;
            await response.WriteStringAsync("OpenAPI source document not found. Run npm run openapi:sync from the repository root.");
            return response;
        }

        response.StatusCode = HttpStatusCode.OK;
        response.Headers.Add("Content-Type", "application/json; charset=utf-8");
        var json = await File.ReadAllTextAsync(SourceDocumentPath);
        await response.WriteStringAsync(json);
        return response;
    }
}
