using System.Net;
using API.Utils;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Services;

namespace API.Endpoints.GarminCourses;

internal static class GarminCourseAuthorization
{
    private const string AllowedUserId = "11908635";

    public static async Task<HttpResponseData?> Authorize(
        HttpRequestData req,
        UserAuthenticationService userAuthService,
        string methods = "GET, POST, OPTIONS")
    {
        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
        {
            var unauthorizedResponse = req.CreateResponse(HttpStatusCode.Unauthorized);
            CorsHeaders.Add(req, unauthorizedResponse, methods);
            return unauthorizedResponse;
        }

        if (!string.Equals(user.Id, AllowedUserId, StringComparison.Ordinal))
        {
            var forbiddenResponse = req.CreateResponse(HttpStatusCode.Forbidden);
            CorsHeaders.Add(req, forbiddenResponse, methods);
            return forbiddenResponse;
        }

        return null;
    }
}