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
            return CreateProxyUnavailable(req, methods);
        }
    }

    internal static HttpResponseData CreateProxyUnavailable(HttpRequestData req, string methods)
    {
        var response = req.CreateResponse(HttpStatusCode.BadGateway);
        CorsHeaders.Add(req, response, methods);
        response.WriteString("Garmin proxy service is unavailable. Configure GarminProxyBaseUrl or start the local proxy service.");
        return response;
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
            || headerName.Equals("Upgrade", StringComparison.OrdinalIgnoreCase)
            || headerName.StartsWith("Access-Control-", StringComparison.OrdinalIgnoreCase);
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
    internal const long HardcodedDeviceId = 3616062752L;

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

        HttpResponseMessage upstreamResponse;
        try
        {
            upstreamResponse = await garminCoursesApi.UpdateCourse(courseId, gpxBytes, fileName, cancellationToken);
        }
        catch (HttpRequestException)
        {
            return GetGarminCourses.CreateProxyUnavailable(req, "DELETE, PUT, OPTIONS");
        }

        using (upstreamResponse)
        {
            if (upstreamResponse.IsSuccessStatusCode)
            {
                if (long.TryParse(courseId, out long courseIdLong))
                {
                    var courseName = System.IO.Path.GetFileNameWithoutExtension(fileName);
                    var messages = new[]
                    {
                        new
                        {
                            deviceId = HardcodedDeviceId,
                            messageUrl = $"course-service/course/fit/{courseIdLong}/{HardcodedDeviceId}?elevation=true",
                            messageType = "courses",
                            messageName = courseName,
                            groupName = (string?)null,
                            priority = 0,
                            fileType = "FIT",
                            metaDataId = courseIdLong,
                            wifiSetup = true
                        }
                    };

                    _ = garminCoursesApi.SendDeviceMessages(messages, cancellationToken);
                }
            }

            return await GetGarminCourses.ProxyResponseAsync(req, upstreamResponse, cancellationToken, "DELETE, PUT, OPTIONS");
        }
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

        HttpResponseMessage upstreamResponse;
        try
        {
            upstreamResponse = await garminCoursesApi.UploadCourse(gpxBytes, fileName, cancellationToken);
        }
        catch (HttpRequestException)
        {
            return GetGarminCourses.CreateProxyUnavailable(req, "GET, POST, OPTIONS");
        }

        using (upstreamResponse)
        {
            if (upstreamResponse.IsSuccessStatusCode)
            {
                var responseBody = await upstreamResponse.Content.ReadAsByteArrayAsync(cancellationToken);

                if (TryExtractCourseId(responseBody, out var courseIdLong))
                {
                    var courseName = Path.GetFileNameWithoutExtension(fileName);
                    var messages = new[]
                    {
                        new
                        {
                            deviceId = PutGarminCourse.HardcodedDeviceId,
                            messageUrl = $"course-service/course/fit/{courseIdLong}/{PutGarminCourse.HardcodedDeviceId}?elevation=true",
                            messageType = "courses",
                            messageName = courseName,
                            groupName = (string?)null,
                            priority = 0,
                            fileType = "FIT",
                            metaDataId = courseIdLong,
                            wifiSetup = true
                        }
                    };

                    _ = garminCoursesApi.SendDeviceMessages(messages, cancellationToken);
                }

                // Re-build response from buffered body since the stream is consumed
                var proxyResponse = req.CreateResponse(upstreamResponse.StatusCode);
                CorsHeaders.Add(req, proxyResponse, "GET, POST, OPTIONS");
                foreach (var header in upstreamResponse.Content.Headers)
                    proxyResponse.Headers.TryAddWithoutValidation(header.Key, header.Value);
                await proxyResponse.Body.WriteAsync(responseBody, cancellationToken);
                return proxyResponse;
            }

            return await GetGarminCourses.ProxyResponseAsync(req, upstreamResponse, cancellationToken, "GET, POST, OPTIONS");
        }
    }

    private static bool TryExtractCourseId(byte[] responseBody, out long courseId)
    {
        courseId = 0;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(responseBody);
            var root = doc.RootElement;
            foreach (var name in new[] { "courseId", "id", "metaDataId" })
            {
                if (root.TryGetProperty(name, out var prop) && prop.TryGetInt64(out courseId))
                    return true;
            }
        }
        catch (System.Text.Json.JsonException) { }
        return false;
    }
}

public class PostGarminDeviceMessages(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    [OpenApiOperation(tags: ["Garmin Courses"], Summary = "Send device messages to a Garmin device via the configured Garmin proxy service.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiRequestBody(contentType: "application/json", bodyType: typeof(object[]), Required = true, Description = "Array of device message objects.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "CORS preflight response")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(object), Description = "Device messages response.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest, Description = "Body missing or not a JSON array")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [Function(nameof(PostGarminDeviceMessages))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", "options", Route = "garmin/device-messages")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (CorsHeaders.IsOptions(req))
        {
            var optionsResponse = req.CreateResponse(HttpStatusCode.NoContent);
            CorsHeaders.Add(req, optionsResponse, "POST, OPTIONS");
            return optionsResponse;
        }

        var authFailure = await GarminCourseAuthorization.Authorize(req, userAuthService, "POST, OPTIONS");
        if (authFailure is not null)
            return authFailure;

        await using var buffer = new MemoryStream();
        await req.Body.CopyToAsync(buffer, cancellationToken);
        var bodyBytes = buffer.ToArray();
        if (bodyBytes.Length == 0)
            return GetGarminCourses.CreateBadRequest(req, "Request body must contain a JSON array of device messages", "POST, OPTIONS");

        var content = new System.Net.Http.ByteArrayContent(bodyBytes);
        content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse("application/json");

        return await GetGarminCourses.ProxyUpstreamAsync(
            req,
            () => garminCoursesApi.SendDeviceMessagesRaw(content, cancellationToken),
            cancellationToken,
            "POST, OPTIONS");
    }
}