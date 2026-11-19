using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using Stigar.Api.Utils;

namespace Stigar.Api.Endpoints.OpenApi;

public class GetSourceOpenApiDocument
{
    private static readonly string SourceDocumentPath = Path.Combine(AppContext.BaseDirectory, "openapi", "openapi.source.json");
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapMethods("/api/openapi/source.json", ["GET", "OPTIONS"],
            (HttpContext context, [FromServices] GetSourceOpenApiDocument endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        if (context.IsOptionsRequest())
            return TypedResults.NoContent();

        if (!File.Exists(SourceDocumentPath))
            return TypedResults.NotFound("OpenAPI source document not found. Run npm run openapi:sync from the repository root.");

        var json = await File.ReadAllTextAsync(SourceDocumentPath);
        return TypedResults.Text(json, "application/json; charset=utf-8");
    }
}
