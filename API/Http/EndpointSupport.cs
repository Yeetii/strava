using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.Configuration;

namespace API.Http;

/// <summary>
/// Small native helpers shared by the minimal API endpoint handlers. They keep
/// request/response handling in ASP.NET Core terms; there is no Functions
/// request/response emulation layer.
/// </summary>
public static class EndpointSupport
{
    public static bool IsAdminAuthorized(this HttpContext context, IConfiguration configuration)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
            return false;

        return context.Request.Headers.TryGetValue("x-admin-key", out var provided)
               && provided.FirstOrDefault() == adminKey;
    }

    public static string? SessionId(this HttpContext context) =>
        context.Request.Cookies["session"];

    public static bool IsOptionsRequest(this HttpContext context) =>
        HttpMethods.IsOptions(context.Request.Method);

    public static IResult Text(string text, int statusCode = StatusCodes.Status200OK) =>
        TypedResults.Text(text, statusCode: statusCode);

    public static IResult Json<TValue>(TValue value, int statusCode = StatusCodes.Status200OK) =>
        TypedResults.Json(value, statusCode: statusCode);
}
