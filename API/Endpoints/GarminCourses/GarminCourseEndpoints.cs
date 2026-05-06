using System.Net;
using API.Utils;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;
using Shared.Services.GarminClient;

namespace API.Endpoints.GarminCourses;

public class GetGarminCourses(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    [OpenApiOperation(tags: ["Garmin Courses"], Summary = "List Garmin courses from the configured Garmin proxy service.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "type", In = ParameterLocation.Query, Type = typeof(string), Required = false)]
    [OpenApiParameter(name: "start", In = ParameterLocation.Query, Type = typeof(int), Required = false)]
    [OpenApiParameter(name: "limit", In = ParameterLocation.Query, Type = typeof(int), Required = false)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "CORS preflight response")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(object), Description = "Garmin courses response.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest, Description = "Invalid query parameters")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [Function(nameof(GetGarminCourses))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", "options", Route = "garmin/courses")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (CorsHeaders.IsOptions(req))
        {
            var optionsResponse = req.CreateResponse(HttpStatusCode.NoContent);
            CorsHeaders.Add(req, optionsResponse, "GET, POST, OPTIONS");
            return optionsResponse;
        }

        var authFailure = await GarminCourseAuthorization.Authorize(req, userAuthService);
        if (authFailure is not null)
            return authFailure;

        if (!TryParsePagination(req, out var start, out var limit, out var badRequest))
            return badRequest!;

        return await ProxyUpstreamAsync(
            req,
            () => garminCoursesApi.GetCourses(req.Query["type"], start, limit, cancellationToken),
            cancellationToken,
            "GET, POST, OPTIONS");
    }

    internal static bool TryParsePagination(HttpRequestData req, out int start, out int limit, out HttpResponseData? badRequest)
    {
        start = 0;
        limit = 50;
        badRequest = null;

        var startValue = req.Query["start"];
        if (!string.IsNullOrWhiteSpace(startValue) && !int.TryParse(startValue, out start))
        {
            badRequest = CreateBadRequest(req, "start must be an integer", "GET, POST, OPTIONS");
            return false;
        }

        var limitValue = req.Query["limit"];
        if (!string.IsNullOrWhiteSpace(limitValue) && !int.TryParse(limitValue, out limit))
        {
            badRequest = CreateBadRequest(req, "limit must be an integer", "GET, POST, OPTIONS");
            return false;
        }

        return true;
    }

    internal static HttpResponseData CreateBadRequest(HttpRequestData req, string message, string methods)
    {
        var response = req.CreateResponse(HttpStatusCode.BadRequest);
        CorsHeaders.Add(req, response, methods);
        response.WriteString(message);
        return response;
    }

    internal static async Task<HttpResponseData> ProxyUpstreamAsync(
        HttpRequestData req,
        Func<Task<HttpResponseMessage>> upstreamCall,
        CancellationToken cancellationToken,
        string methods)
    {
        try
        {
            using var upstreamResponse = await upstreamCall();
            return await ProxyResponseAsync(req, upstreamResponse, cancellationToken, methods);
        }
        catch (HttpRequestException)
        {
            var response = req.CreateResponse(HttpStatusCode.BadGateway);
            CorsHeaders.Add(req, response, methods);
            return response;
        }
    }

    internal static async Task<HttpResponseData> ProxyResponseAsync(
        HttpRequestData req,
        HttpResponseMessage upstreamResponse,
        CancellationToken cancellationToken,
        string methods)
    {
        var response = req.CreateResponse(upstreamResponse.StatusCode);
        CorsHeaders.Add(req, response, methods);

        foreach (var header in upstreamResponse.Headers)
        {
            if (IsHopByHopHeader(header.Key))
                continue;

            response.Headers.Add(header.Key, string.Join(",", header.Value));
        }

        foreach (var header in upstreamResponse.Content.Headers)
        {
            if (IsHopByHopHeader(header.Key))
                continue;

            response.Headers.Add(header.Key, string.Join(",", header.Value));
        }

        await upstreamResponse.Content.CopyToAsync(response.Body, cancellationToken);
        return response;
    }

    private static bool IsHopByHopHeader(string headerName)
    {
        return headerName.Equals("Transfer-Encoding", StringComparison.OrdinalIgnoreCase)
            || headerName.Equals("Connection", StringComparison.OrdinalIgnoreCase)
            || headerName.Equals("Keep-Alive", StringComparison.OrdinalIgnoreCase)
            || headerName.Equals("Proxy-Authenticate", StringComparison.OrdinalIgnoreCase)
            || headerName.Equals("Proxy-Authorization", StringComparison.OrdinalIgnoreCase)
            || headerName.Equals("TE", StringComparison.OrdinalIgnoreCase)
            || headerName.Equals("Trailer", StringComparison.OrdinalIgnoreCase)
            || headerName.Equals("Upgrade", StringComparison.OrdinalIgnoreCase);
    }
}

public class GetGarminCourse(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    [OpenApiOperation(tags: ["Garmin Courses"], Summary = "Get a Garmin course from the configured Garmin proxy service.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "courseId", In = ParameterLocation.Path, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(object), Description = "Garmin course response.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [Function(nameof(GetGarminCourse))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "garmin/courses/{courseId}")] HttpRequestData req,
        string courseId,
        CancellationToken cancellationToken)
    {
        var authFailure = await GarminCourseAuthorization.Authorize(req, userAuthService, "GET, OPTIONS");
        if (authFailure is not null)
            return authFailure;

        return await GetGarminCourses.ProxyUpstreamAsync(
            req,
            () => garminCoursesApi.GetCourse(courseId, cancellationToken),
            cancellationToken,
            "GET, OPTIONS");
    }
}

public class GetGarminCourseGpx(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    [OpenApiOperation(tags: ["Garmin Courses"], Summary = "Download a Garmin course GPX from the configured Garmin proxy service.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "courseId", In = ParameterLocation.Path, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/gpx+xml", bodyType: typeof(string), Description = "Garmin course GPX.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [Function(nameof(GetGarminCourseGpx))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "garmin/courses/{courseId}/gpx")] HttpRequestData req,
        string courseId,
        CancellationToken cancellationToken)
    {
        var authFailure = await GarminCourseAuthorization.Authorize(req, userAuthService, "GET, OPTIONS");
        if (authFailure is not null)
            return authFailure;

        return await GetGarminCourses.ProxyUpstreamAsync(
            req,
            () => garminCoursesApi.DownloadCourseGpx(courseId, cancellationToken),
            cancellationToken,
            "GET, OPTIONS");
    }
}

public class DeleteGarminCourse(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    [OpenApiOperation(tags: ["Garmin Courses"], Summary = "Delete a Garmin course via the configured Garmin proxy service.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "courseId", In = ParameterLocation.Path, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(object), Description = "Garmin course delete response.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [Function(nameof(DeleteGarminCourse))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "delete", "options", Route = "garmin/courses/{courseId}")] HttpRequestData req,
        string courseId,
        CancellationToken cancellationToken)
    {
        if (CorsHeaders.IsOptions(req))
        {
            var optionsResponse = req.CreateResponse(HttpStatusCode.NoContent);
            CorsHeaders.Add(req, optionsResponse, "DELETE, PUT, OPTIONS");
            return optionsResponse;
        }

        var authFailure = await GarminCourseAuthorization.Authorize(req, userAuthService, "DELETE, PUT, OPTIONS");
        if (authFailure is not null)
            return authFailure;

        return await GetGarminCourses.ProxyUpstreamAsync(
            req,
            () => garminCoursesApi.DeleteCourse(courseId, cancellationToken),
            cancellationToken,
            "DELETE, PUT, OPTIONS");
    }
}

public class PutGarminCourse(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    [OpenApiOperation(tags: ["Garmin Courses"], Summary = "Replace a Garmin course's route with new GPX via the configured Garmin proxy service.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "courseId", In = ParameterLocation.Path, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "filename", In = ParameterLocation.Query, Type = typeof(string), Required = false)]
    [OpenApiRequestBody(contentType: "application/gpx+xml", bodyType: typeof(string), Required = true)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "CORS preflight response")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(object), Description = "Garmin course update response.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest, Description = "GPX payload missing")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [Function(nameof(PutGarminCourse))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "put", Route = "garmin/courses/{courseId}")] HttpRequestData req,
        string courseId,
        CancellationToken cancellationToken)
    {
        var authFailure = await GarminCourseAuthorization.Authorize(req, userAuthService, "DELETE, PUT, OPTIONS");
        if (authFailure is not null)
            return authFailure;

        await using var buffer = new MemoryStream();
        await req.Body.CopyToAsync(buffer, cancellationToken);
        var gpxBytes = buffer.ToArray();
        if (gpxBytes.Length == 0)
            return GetGarminCourses.CreateBadRequest(req, "Request body must contain GPX data", "DELETE, PUT, OPTIONS");

        var fileName = req.Query["filename"];
        if (string.IsNullOrWhiteSpace(fileName))
            fileName = "course.gpx";

        return await GetGarminCourses.ProxyUpstreamAsync(
            req,
            () => garminCoursesApi.UpdateCourse(courseId, gpxBytes, fileName, cancellationToken),
            cancellationToken,
            "DELETE, PUT, OPTIONS");
    }
}

public class PostGarminCourse(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    [OpenApiOperation(tags: ["Garmin Courses"], Summary = "Upload a Garmin course GPX via the configured Garmin proxy service.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "filename", In = ParameterLocation.Query, Type = typeof(string), Required = false)]
    [OpenApiRequestBody(contentType: "application/gpx+xml", bodyType: typeof(string), Required = true)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "CORS preflight response")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(object), Description = "Garmin course upload response.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest, Description = "GPX payload missing")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [Function(nameof(PostGarminCourse))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "garmin/courses")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        var authFailure = await GarminCourseAuthorization.Authorize(req, userAuthService);
        if (authFailure is not null)
            return authFailure;

        await using var buffer = new MemoryStream();
        await req.Body.CopyToAsync(buffer, cancellationToken);
        var gpxBytes = buffer.ToArray();
        if (gpxBytes.Length == 0)
            return GetGarminCourses.CreateBadRequest(req, "Request body must contain GPX data", "GET, POST, OPTIONS");

        var fileName = req.Query["filename"];
        if (string.IsNullOrWhiteSpace(fileName))
            fileName = "course.gpx";

        return await GetGarminCourses.ProxyUpstreamAsync(
            req,
            () => garminCoursesApi.UploadCourse(gpxBytes, fileName, cancellationToken),
            cancellationToken,
            "GET, POST, OPTIONS");
    }
}