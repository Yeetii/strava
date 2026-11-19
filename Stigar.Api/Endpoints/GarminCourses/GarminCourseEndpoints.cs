using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Stigar.Core.Services;
using Stigar.Core.Services.GarminClient;

namespace Stigar.Api.Endpoints.GarminCourses;

public class GetGarminCourses(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapMethods("/api/garmin/courses", ["GET", "OPTIONS"],
            (HttpContext context, [FromServices] GetGarminCourses endpoint) => endpoint.Handle(context, context.RequestAborted));

    // Registers every Garmin course endpoint in this file. The host calls this
    // once during startup; keeping it here makes the route/verb set explicit.
    public static void MapAll(IEndpointRouteBuilder app)
    {
        Map(app);
        GetGarminCourse.Map(app);
        GetGarminCourseGpx.Map(app);
        DeleteGarminCourse.Map(app);
        PutGarminCourse.Map(app);
        PostGarminCourse.Map(app);
        PostGarminDeviceMessages.Map(app);
    }

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (context.IsOptionsRequest())
            return TypedResults.NoContent();

        var authFailure = await GarminCourseAuthorization.Authorize(context, userAuthService);
        if (authFailure is not null)
            return authFailure;

        if (!TryParsePagination(context, out var start, out var limit, out var badRequest))
            return badRequest!;

        return await ProxyUpstreamAsync(
            context,
            () => garminCoursesApi.GetCourses(context.Request.Query["type"], start, limit, cancellationToken),
            cancellationToken);
    }

    internal static bool TryParsePagination(HttpContext context, out int start, out int limit, out IResult? badRequest)
    {
        start = 0;
        limit = 50;
        badRequest = null;

        var startValue = context.Request.Query["start"];
        if (!string.IsNullOrWhiteSpace(startValue) && !int.TryParse(startValue, out start))
        {
            badRequest = CreateBadRequest(context, "start must be an integer");
            return false;
        }

        var limitValue = context.Request.Query["limit"];
        if (!string.IsNullOrWhiteSpace(limitValue) && !int.TryParse(limitValue, out limit))
        {
            badRequest = CreateBadRequest(context, "limit must be an integer");
            return false;
        }

        return true;
    }

    internal static IResult CreateBadRequest(HttpContext context, string message) =>
        EndpointSupport.Text(message, StatusCodes.Status400BadRequest);

    internal static async Task<IResult> ProxyUpstreamAsync(
        HttpContext context,
        Func<Task<HttpResponseMessage>> upstreamCall,
        CancellationToken cancellationToken)
    {
        try
        {
            using var upstreamResponse = await upstreamCall();
            return await ProxyResponseAsync(context, upstreamResponse, cancellationToken);
        }
        catch (HttpRequestException)
        {
            return CreateProxyUnavailable(context);
        }
    }

    internal static IResult CreateProxyUnavailable(HttpContext context) =>
        EndpointSupport.Text(
            "Garmin proxy service is unavailable. Configure GarminProxyBaseUrl or start the local proxy service.",
            StatusCodes.Status502BadGateway);

    internal static async Task<IResult> ProxyResponseAsync(
        HttpContext context,
        HttpResponseMessage upstreamResponse,
        CancellationToken cancellationToken)
    {
        context.Response.StatusCode = (int)upstreamResponse.StatusCode;

        foreach (var header in upstreamResponse.Headers)
        {
            if (IsHopByHopHeader(header.Key))
                continue;

            context.Response.Headers.Append(header.Key, string.Join(",", header.Value));
        }

        foreach (var header in upstreamResponse.Content.Headers)
        {
            if (IsHopByHopHeader(header.Key))
                continue;

            context.Response.Headers.Append(header.Key, string.Join(",", header.Value));
        }

        await upstreamResponse.Content.CopyToAsync(context.Response.Body, cancellationToken);
        return TypedResults.Empty;
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
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/garmin/courses/{courseId}",
            (HttpContext context, [FromServices] GetGarminCourse endpoint, string courseId) => endpoint.Handle(context, courseId, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string courseId, CancellationToken cancellationToken)
    {
        var authFailure = await GarminCourseAuthorization.Authorize(context, userAuthService);
        if (authFailure is not null)
            return authFailure;

        return await GetGarminCourses.ProxyUpstreamAsync(
            context,
            () => garminCoursesApi.GetCourse(courseId, cancellationToken),
            cancellationToken);
    }
}

public class GetGarminCourseGpx(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/garmin/courses/{courseId}/gpx",
            (HttpContext context, [FromServices] GetGarminCourseGpx endpoint, string courseId) => endpoint.Handle(context, courseId, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string courseId, CancellationToken cancellationToken)
    {
        var authFailure = await GarminCourseAuthorization.Authorize(context, userAuthService);
        if (authFailure is not null)
            return authFailure;

        return await GetGarminCourses.ProxyUpstreamAsync(
            context,
            () => garminCoursesApi.DownloadCourseGpx(courseId, cancellationToken),
            cancellationToken);
    }
}

public class DeleteGarminCourse(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapMethods("/api/garmin/courses/{courseId}", ["DELETE", "OPTIONS"],
            (HttpContext context, [FromServices] DeleteGarminCourse endpoint, string courseId) => endpoint.Handle(context, courseId, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string courseId, CancellationToken cancellationToken)
    {
        if (context.IsOptionsRequest())
            return TypedResults.NoContent();

        var authFailure = await GarminCourseAuthorization.Authorize(context, userAuthService);
        if (authFailure is not null)
            return authFailure;

        return await GetGarminCourses.ProxyUpstreamAsync(
            context,
            () => garminCoursesApi.DeleteCourse(courseId, cancellationToken),
            cancellationToken);
    }
}

public class PutGarminCourse(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    internal const long HardcodedDeviceId = 3616062752L;

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPut("/api/garmin/courses/{courseId}",
            (HttpContext context, [FromServices] PutGarminCourse endpoint, string courseId) => endpoint.Handle(context, courseId, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string courseId, CancellationToken cancellationToken)
    {
        var authFailure = await GarminCourseAuthorization.Authorize(context, userAuthService);
        if (authFailure is not null)
            return authFailure;

        await using var buffer = new MemoryStream();
        await context.Request.Body.CopyToAsync(buffer, cancellationToken);
        var gpxBytes = buffer.ToArray();
        if (gpxBytes.Length == 0)
            return GetGarminCourses.CreateBadRequest(context, "Request body must contain GPX data");

        var fileName = context.Request.Query["filename"];
        if (string.IsNullOrWhiteSpace(fileName))
            fileName = "course.gpx";

        HttpResponseMessage upstreamResponse;
        try
        {
            upstreamResponse = await garminCoursesApi.UpdateCourse(courseId, gpxBytes, fileName, cancellationToken);
        }
        catch (HttpRequestException)
        {
            return GetGarminCourses.CreateProxyUnavailable(context);
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

            return await GetGarminCourses.ProxyResponseAsync(context, upstreamResponse, cancellationToken);
        }
    }
}

public class PostGarminCourse(
    UserAuthenticationService userAuthService,
    GarminCoursesApi garminCoursesApi)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/garmin/courses",
            (HttpContext context, [FromServices] PostGarminCourse endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        var authFailure = await GarminCourseAuthorization.Authorize(context, userAuthService);
        if (authFailure is not null)
            return authFailure;

        await using var buffer = new MemoryStream();
        await context.Request.Body.CopyToAsync(buffer, cancellationToken);
        var gpxBytes = buffer.ToArray();
        if (gpxBytes.Length == 0)
            return GetGarminCourses.CreateBadRequest(context, "Request body must contain GPX data");

        var fileName = context.Request.Query["filename"];
        if (string.IsNullOrWhiteSpace(fileName))
            fileName = "course.gpx";

        HttpResponseMessage upstreamResponse;
        try
        {
            upstreamResponse = await garminCoursesApi.UploadCourse(gpxBytes, fileName, cancellationToken);
        }
        catch (HttpRequestException)
        {
            return GetGarminCourses.CreateProxyUnavailable(context);
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
                context.Response.StatusCode = (int)upstreamResponse.StatusCode;
                foreach (var header in upstreamResponse.Content.Headers)
                    context.Response.Headers.Append(header.Key, string.Join(",", header.Value));
                await context.Response.Body.WriteAsync(responseBody, cancellationToken);
                return TypedResults.Empty;
            }

            return await GetGarminCourses.ProxyResponseAsync(context, upstreamResponse, cancellationToken);
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
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapMethods("/api/garmin/device-messages", ["POST", "OPTIONS"],
            (HttpContext context, [FromServices] PostGarminDeviceMessages endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (context.IsOptionsRequest())
            return TypedResults.NoContent();

        var authFailure = await GarminCourseAuthorization.Authorize(context, userAuthService);
        if (authFailure is not null)
            return authFailure;

        await using var buffer = new MemoryStream();
        await context.Request.Body.CopyToAsync(buffer, cancellationToken);
        var bodyBytes = buffer.ToArray();
        if (bodyBytes.Length == 0)
            return GetGarminCourses.CreateBadRequest(context, "Request body must contain a JSON array of device messages");

        var content = new System.Net.Http.ByteArrayContent(bodyBytes);
        content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse("application/json");

        return await GetGarminCourses.ProxyUpstreamAsync(
            context,
            () => garminCoursesApi.SendDeviceMessagesRaw(content, cancellationToken),
            cancellationToken);
    }
}
