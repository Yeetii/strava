using API.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Shared.Services;

namespace API.Endpoints.GarminCourses;

internal static class GarminCourseAuthorization
{
    private const string AllowedUserId = "11908635";

    public static async Task<IResult?> Authorize(
        HttpContext context,
        UserAuthenticationService userAuthService)
    {
        var user = await userAuthService.GetUserFromSessionId(context.SessionId());
        if (user == null)
            return TypedResults.Unauthorized();

        if (!string.Equals(user.Id, AllowedUserId, StringComparison.Ordinal))
            return TypedResults.StatusCode(StatusCodes.Status403Forbidden);

        return null;
    }
}
